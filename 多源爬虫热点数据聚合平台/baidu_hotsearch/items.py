# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class BaiduHotsearchItem(scrapy.Item):
    type_ = scrapy.Field()  # news/novel/movie
    ranking = scrapy.Field()
    title = scrapy.Field()
    link = scrapy.Field()
    hot_index = scrapy.Field()
    cover_url = scrapy.Field()
    movie_type = scrapy.Field()
    actors = scrapy.Field()
    description = scrapy.Field()