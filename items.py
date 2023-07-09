# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Hello2Item(scrapy.Item):
    # define the fields for your item here like:
    header = scrapy.Field()
    name = scrapy.Field()
    country = scrapy.Field()
    time = scrapy.Field()
    verify = scrapy.Field()
    comment = scrapy.Field()
    review_value = scrapy.Field()
    rating = scrapy.Field()
