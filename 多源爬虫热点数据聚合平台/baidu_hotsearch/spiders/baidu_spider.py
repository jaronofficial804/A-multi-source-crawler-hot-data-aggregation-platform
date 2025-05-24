import scrapy
from scrapy_redis.spiders import RedisSpider
from baidu_hotsearch.items import BaiduHotsearchItem
from urllib.parse import urlparse


class BaiduSpider(RedisSpider):
    name = 'baidu_hot'
    redis_key = 'baidu:start_urls'

    # 初始URLs
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = [
            "https://top.baidu.com/board?tab=realtime",
            "https://top.baidu.com/board?tab=novel",
            "https://top.baidu.com/board?tab=movie"
        ]

    def parse(self, response):
        url = response.url
        parsed_url = urlparse(url)
        query = parsed_url.query
        type_ = 'news'

        if 'tab=novel' in query:
            type_ = 'novel'
        elif 'tab=movie' in query:
            type_ = 'movie'

        items = response.css('div.category-wrap_iQLoo')

        for index, item in enumerate(items, 1):
            try:
                baidu_item = BaiduHotsearchItem()
                baidu_item['type_'] = type_
                baidu_item['ranking'] = index

                title = item.css('a.title_dIF3B div.c-single-text-ellipsis::text').get()
                baidu_item['title'] = title.strip() if title else '无标题'

                link = item.css('a.title_dIF3B::attr(href)').get()
                baidu_item['link'] = link if link else '无链接'

                hot_index = item.css('div.hot-index_1Bl1a::text').get()
                baidu_item['hot_index'] = hot_index.strip() if hot_index else '无热度'

                cover_url = item.css('a.img-wrapper_29V76 img::attr(src)').get()
                baidu_item['cover_url'] = cover_url if cover_url else '无封面'

                if type_ == "movie":
                    movie_type = item.css('div.intro_1l0wp::text').get()
                    baidu_item['movie_type'] = movie_type.replace('类型：', '').strip() if movie_type else '无类型'

                    actors = item.xpath(
                        './/div[contains(@class, "intro_1l0wp") and contains(text(), "演员：")]/text()').get()
                    baidu_item['actors'] = actors.replace('演员：', '').strip() if actors else '无演员信息'

                    description = item.css('div.desc_3CTjT::text').get()
                    baidu_item['description'] = description.strip() if description else '无描述'

                yield baidu_item
            except Exception as e:
                self.logger.error(f"解析错误: {e}")