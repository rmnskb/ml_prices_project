import scrapy
from scrapy import Request
from scrapy.http import Response
from typing import Iterable, Any
from ..items import IDItem


class IDSpider(scrapy.Spider):
    name = 'estate_ids'
    custom_settings = {
        'ITEM_PIPELINES': {
            "estates_scraping.pipelines.IDScrapingPipeline": 300
        }
    }

    def start_requests(self) -> Iterable[Request]:
        url = 'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_type_cb=2&per_page=100&page=1'

        yield scrapy.Request(url)

    def parse(self, response: Response, **kwargs: Any) -> Any:
        result_size = response.json()['result_size']
        per_page = response.json()['per_page']
        number_of_pages = -(result_size // -per_page)  # ceiling division
        page = response.json()['page']
        id_item = IDItem()

        for estate in response.json()['_embedded']['estates']:
            id_item['id'] = estate['hash_id']
            id_item['name'] = estate['name']
            id_item['price'] = estate['price']
            yield id_item

        next_page = f'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_type_cb=2&per_page=100&page={page + 1}'
        page += 1

        if page <= number_of_pages:
            yield scrapy.Request(next_page, callback=self.parse)
