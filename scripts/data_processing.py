import polars as pl
import polars.selectors as cs
from sqlalchemy import create_engine
# from scrape.estates_scraping.db.db import get_connection
from db.utils import get_connection


class DataProcessor:
    def __init__(self):
        self._sql_engine = create_engine(get_connection())
        self._rooms_translation = {
            2: '1kk'
            , 3: '1+1'
            , 4: '2kk'
            , 5: '2+1'
            , 6: '3kk'
            , 7: '3+1'
            , 8: '4kk'
            , 9: '4+1'
            , 10: '5kk'
            , 11: '5+1'
            , 12: '6kk+'
            , 16: 'Atypický'
            , 37: 'Atypický'
            , 47: 'Pokoj'
        }

    def _translate_dict(self, code: int, dictionary: dict) -> str:
        return dictionary.get(code, code)

    def _has_separate_kitchen(self, room: str) -> int:
        if '+' in room:
            return 1
        else:
            return 0

    def _process_rooms(self, room: str) -> int:
        rooms_to_int = {
            'Atypický': 1
            , 'Pokoj': 1
        }

        if room in rooms_to_int.keys():
            return rooms_to_int[room]
        elif self._has_separate_kitchen(room=room):
            return int(room[0]) + 1
        else:
            return int(room[0])

    def _get_raw_data(self) -> pl.DataFrame:
        with self._sql_engine.connect() as connection:
            df = pl.read_database(
                query="""
                    SELECT 
                        fd.flat_id
                        , fd.usable_area
                        , fd.rooms
                        , fd.furnished
                        , fd.parking_lot
                        , fd.terrace
                        , fd.balcony
                        , fd.loggia
                        , fd.elevator
                        , fd.cellar
                        , fd.easy_access
                        , fd.garage
                        , fd.structure_type
                        , fd.floor
                        , fd.estate_state
                        , fg.postal_cd
                        , COALESCE(fg.district, fg.city) district -- impute the city name if the district's missing 
                        , fg.city
                        , fg.county
                        , fg.state 
                        , fg.longitude
                        , fg.latitude
                        , ROUND(COALESCE(fp.avg_distance, 0), 2) avg_distance
                        , ROUND(COALESCE(fp.avg_rating, 0), 2) avg_rating
                        , ROUND(COALESCE(fpt.min_distance, 0), 2) min_distance_pt -- minimum distance to public transportation
                        , fd.update_dt
                        , fd.price
                    FROM flats_data fd
                    LEFT JOIN flats_geo fg ON fd.flat_id = fg.flat_id
                    LEFT JOIN (
                        SELECT jc.flat_id
                             , AVG(fp.distance) avg_distance
                             , AVG(fp.rating)   avg_rating
                        FROM flats_poi_junction jc
                        LEFT JOIN flats_poi fp ON jc.poi_id = fp.poi_id
                        WHERE fp.rating <> -1 AND name NOT IN ('Metro', 'Vlak', 'Bus MHD', 'Tram')
                        GROUP BY jc.flat_id
                    ) fp ON fp.flat_id = fd.flat_id
                    LEFT JOIN (
                        SELECT jc.flat_id
                            , MIN(fp.distance) min_distance
                        FROM flats_poi_junction jc
                        LEFT JOIN flats_poi fp ON jc.poi_id = fp.poi_id
                        WHERE name IN ('Metro', 'Vlak', 'Bus MHD', 'Tram')
                        GROUP BY jc.flat_id
                    ) fpt on fpt.flat_id = fd.flat_id 
                """
                , connection=connection
            )

        return df

    def _preprocess_data(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df
            .filter((df['usable_area'] < df['usable_area'].quantile(0.999)) & (df['usable_area'] > 0))
            .with_columns(
                rooms=pl.col('rooms').map_elements(
                    lambda code: self._translate_dict(code=code, dictionary=self._rooms_translation)
                    , return_dtype=pl.datatypes.String
                )
                , elevator=pl.col('elevator').replace(2, 0)
                , floor=(pl.col('floor').str.split(' ').list.get(0).replace('přízemí', '0.')
                         .str.replace_all(r'[^0-9]', '').cast(pl.Int64))
            )
            .filter(pl.col('floor') <= pl.col('floor').quantile(0.9995))
        )

    def _process_features(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df
            .with_columns(
                rooms_int=pl.col('rooms').map_elements(lambda room: self._process_rooms(room=room),
                                                       return_dtype=pl.Int64)
                , has_separate_kitchen=pl.col('rooms')
                .map_elements(lambda room: self._has_separate_kitchen(room=room), return_dtype=pl.Int32)
                , area_sq=(pl.col('usable_area').cast(pl.Float64) ** 2).round(2)
                , avg_distance_sqrt=(pl.col('avg_distance').cast(pl.Float64).sqrt()).round(2)
                , avg_rating_sq=(pl.col('avg_rating').cast(pl.Float64) ** 2).round(2)
                , postal_cd=pl.col('postal_cd').str.replace(' ', '').str
                .replace('-', '').cast(pl.Int64))
            .with_columns(
                [
                    (pl.col('usable_area') / pl.col('rooms_int')).round(2).alias('avg_area_per_room')
                    , (pl.col('rooms_int').cut([2, 5], labels=['Small', 'Medium', 'Large'])).alias('rooms_bins')
                    , (pl.col('floor').cut([3, 6], labels=['Low', 'Medium', 'High'])).alias('floor_bins')
                    , (pl.col('avg_distance').qcut([0.25, 0.75], labels=['Close', 'Medium-Close', 'Far']))
                    .alias('poi_distance_bins')
                    , (pl.col('min_distance_pt').qcut([0.25, 0.75], labels=['Close', 'Medium-Close', 'Far']))
                    .alias('pt_distance_bins')
                    , pl.when((pl.col('elevator') == 1) & (pl.col('floor') > 2))
                        .then(pl.col('floor').log1p())
                        .otherwise(0)
                        .round(2)
                        .alias('elevator_usability')
                    , pl.when((pl.col('parking_lot') == 1) | (pl.col('garage') == 1))
                        .then(1)
                        .otherwise(0)
                        .alias('has_parking_space')
                    , pl.when((pl.col('terrace') == 1) | (pl.col('loggia') == 1) | (pl.col('balcony') == 1))
                        .then(1)
                        .otherwise(0)
                        .alias('has_outside_space')
                ])
            .cast(
                {
                    pl.String: pl.Categorical
                    , pl.Decimal: pl.Float64
                })
        )

    def _post_processed_data(self, df: pl.DataFrame) -> None:
        with self._sql_engine.connect() as connection:
            df.write_database(
                table_name='flats_features'
                , connection=connection
                , if_table_exists='replace'
            )

    def fit(self) -> None:
        raw_df = self._get_raw_data()
        processed_df = self._preprocess_data(df=raw_df)
        featured_df = self._process_features(df=processed_df)

        self._post_processed_data(featured_df)


if __name__ == '__main__':
    dp = DataProcessor()
    dp.fit()
