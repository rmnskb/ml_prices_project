import scrapy
from scrapy import Request
from scrapy.http import Response
from typing import Iterable, Any
from ..items import FeatureItem, GeoItem, POIItem
from ..db.db import check_ids


class FeatureSpider(scrapy.Spider):
    name = 'estate_features'
    custom_settings = {
        'ITEM_PIPELINES': {
            "estates_scraping.pipelines.FeaturesScrapingPipeline": 300,
            "estates_scraping.pipelines.GeoScrapingPipeline": 300,
            "estates_scraping.pipelines.POIScrapingPipeline": 300
        }
    }

    def start_requests(self) -> Iterable[Request]:
        base_url = "https://www.sreality.cz/api/cs/v2/estates/"
        ids = check_ids()

        for id in ids:
            url = f"{base_url}{id}"

            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response: Response, **kwargs: Any) -> Any:
        feature_item = FeatureItem()
        geo_item = GeoItem()
        poi_item = POIItem()

        features_data = response.json()['recommendations_data']
        feature_item['flat_id'] = features_data['hash_id']
        feature_item['usable_area'] = features_data['usable_area']
        feature_item['rooms'] = features_data['category_sub_cb']  # Add translation for the number of the rooms
        feature_item['furnished'] = features_data['furnished']
        feature_item['parking_lot'] = features_data['parking_lots']
        feature_item['terrace'] = features_data['terrace']
        feature_item['balcony'] = features_data['balcony']
        feature_item['loggia'] = features_data['loggia']
        feature_item['elevator'] = features_data['elevator']
        feature_item['cellar'] = features_data['cellar']
        feature_item['basin'] = features_data['basin']
        feature_item['low_energy'] = features_data['low_energy']
        feature_item['easy_access'] = features_data['easy_access']
        feature_item['building_condition'] = features_data['building_condition']
        feature_item['garage'] = features_data['garage']
        feature_item['price'] = features_data['price_summary_czk']

        items_data = response.json()['items']
        target_names = ['Poznámka k ceně', 'Aktualizace', 'Stavba', 'Podlaží', 'Stav objektu']
        db_names = {    # Translation from JSON back to DB names
            'Poznámka k ceně': 'price_note'
            , 'Aktualizace': 'update_dt'
            , 'Stavba': 'structure_type'
            , 'Podlaží': 'floor'
            , 'Stav objektu': 'estate_state'
        }

        for item in items_data:
            if item['name'] in target_names:
                feature_item[db_names[item['name']]] = item['value']

        # Handle missing values
        for db_field in db_names.values():
            if db_field not in feature_item:
                feature_item[db_field] = None

        yield feature_item

        geo_item['flat_id'] = features_data['hash_id']
        geo_item['longitude'] = features_data['locality_gps_lon']
        geo_item['latitude'] = features_data['locality_gps_lat']

        yield geo_item

        for poi in response.json()['poi']:
            poi_item['poi_id'] = poi['source_id']
            poi_item['flat_id'] = features_data['hash_id']
            poi_item['name'] = poi['name']
            poi_item['distance'] = poi['distance']
            poi_item['rating'] = poi['rating']
            poi_item['description'] = poi['description']
            poi_item['review_count'] = poi['review_count']

            yield poi_item



