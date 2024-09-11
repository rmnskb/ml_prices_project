import scrapy


class IDItem(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    price = scrapy.Field()
    type = scrapy.Field()
    checked_flg = scrapy.Field()


class FeatureItem(scrapy.Item):
    flat_id = scrapy.Field()
    usable_area = scrapy.Field()
    rooms = scrapy.Field()
    furnished = scrapy.Field()
    parking_lot = scrapy.Field()
    terrace = scrapy.Field()
    balcony = scrapy.Field()
    loggia = scrapy.Field()
    elevator = scrapy.Field()
    cellar = scrapy.Field()
    basin = scrapy.Field()
    low_energy = scrapy.Field()
    easy_access = scrapy.Field()
    building_condition = scrapy.Field()
    garage = scrapy.Field()
    price = scrapy.Field()
    price_note = scrapy.Field()
    update_dt = scrapy.Field()
    structure_type = scrapy.Field()
    floor = scrapy.Field()
    estate_state = scrapy.Field()


class GeoItem(scrapy.Item):
    flat_id = scrapy.Field()
    longitude = scrapy.Field()
    latitude = scrapy.Field()


class POIItem(scrapy.Item):
    poi_id = scrapy.Field()
    flat_id = scrapy.Field()
    name = scrapy.Field()
    distance = scrapy.Field()
    rating = scrapy.Field()
    description = scrapy.Field()
    review_count = scrapy.Field()
