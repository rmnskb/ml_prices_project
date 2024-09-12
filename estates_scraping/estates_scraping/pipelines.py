from .db.db import get_db
from . import items


class IDScrapingPipeline:
    def process_item(self, item, spider):
        if isinstance(item, items.IDItem):
            with get_db() as db:
                cursor = db.cursor()
                cursor.execute(
                    """
                        SELECT *
                        FROM postgres.public.flats
                        WHERE id = %s
                    """
                    , (item['id'], )
                )
                result = cursor.fetchone()

                if not result:
                    cursor.execute(
                        """
                            INSERT INTO 
                                postgres.public.flats (id, name, price, type, checked_flg) 
                            VALUES (%s, %s, %s, %s, %s)
                        """
                        , (
                            item['id']
                            , item['name']
                            , item['price']
                            , 2
                            , False
                        )
                    )

                    db.commit()

                else:
                    spider.logger.warn(f"Item already in DB: {item['id']}")

        return item


class FeaturesScrapingPipeline:
    def process_item(self, item, spider):
        if isinstance(item, items.FeatureItem):
            with get_db() as db:
                cursor = db.cursor()

                cursor.execute(
                    """
                        SELECT *
                        FROM postgres.public.flats_data
                        WHERE flat_id = %s
                    """
                    , (item['flat_id'], )
                )
                result = cursor.fetchone()

                if not result:
                    cursor.execute(
                        """
                            INSERT INTO postgres.public.flats_data (
                                flat_id, usable_area, rooms, furnished, parking_lot, terrace, balcony, loggia, elevator, cellar
                                , basin, low_energy, easy_access, building_condition, garage, price, price_note, update_dt
                                , structure_type, floor, estate_state
                            )
                            VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """
                        , (
                            item['flat_id']
                            , item['usable_area']
                            , item['rooms']
                            , item['furnished']
                            , item['parking_lot']
                            , item['terrace']
                            , item['balcony']
                            , item['loggia']
                            , item['elevator']
                            , item['cellar']
                            , item['basin']
                            , item['low_energy']
                            , item['easy_access']
                            , item['building_condition']
                            , item['garage']
                            , item['price']
                            , item['price_note']
                            , item['update_dt']  # Parse into dt format
                            , item['structure_type']
                            , item['floor']
                            , item['estate_state']
                        )
                    )

                    cursor.execute(
                        """
                            UPDATE postgres.public.flats
                            SET checked_flg = TRUE
                            WHERE id = %s 
                        """
                        , (item['flat_id'],)
                    )

                    db.commit()

                else:
                    spider.logger.warn(f"Item already in DB: {item['flat_id']}")

        return item


class GeoScrapingPipeline:
    def process_item(self, item, spider):
        if isinstance(item, items.GeoItem):
            with get_db() as db:
                cursor = db.cursor()

                cursor.execute(
                    """
                        SELECT *
                        FROM postgres.public.flats_geo
                        WHERE flat_id = %s
                    """
                    , (item['flat_id'],)
                )
                result = cursor.fetchone()

                if not result:
                    cursor.execute(
                        """
                            INSERT INTO postgres.public.flats_geo (
                                flat_id, longitude, latitude
                            )
                            VALUES (
                                %s, %s, %s
                            )
                        """
                        , (
                            item['flat_id']
                            , item['longitude']
                            , item['latitude']
                        )
                    )

                    db.commit()

                else:
                    spider.logger.warn(f"Item already in DB: {item['flat_id']}")

        return item


class POIScrapingPipeline:
    def process_item(self, item, spider):
        if isinstance(item, items.POIItem):
            with get_db() as db:
                cursor = db.cursor()

                cursor.execute(
                    """
                        SELECT *
                        FROM postgres.public.flats_poi
                        WHERE poi_id = %s
                    """
                    , (item['poi_id'],)
                )
                result = cursor.fetchone()

                if not result:
                    cursor.execute(
                        """
                            INSERT INTO postgres.public.flats_poi (
                                poi_id, name, distance, rating, description, review_count
                            )
                            VALUES (
                                %s, %s, %s, %s, %s, %s
                            )
                        """
                        , (
                            item['poi_id']
                            , item['name']
                            , item['distance']
                            , item['rating']
                            , item['description']
                            , item['review_count']
                        )
                    )
                else:
                    spider.logger.warn(f"Item already in DB: {item['poi_id']}")

                cursor.execute(  # insert the flat - POI relationship to the database regardless
                    """
                        INSERT INTO postgres.public.flats_poi_junction (flat_id, poi_id)
                        VALUES (%s, %s)
                    """
                    , (item['flat_id'], item['poi_id'])
                )

                db.commit()

        return item

