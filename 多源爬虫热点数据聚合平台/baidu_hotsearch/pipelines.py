# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import csv
import mysql.connector
from mysql.connector import Error
from itemadapter import ItemAdapter


class MysqlPipeline:
    def __init__(self, mysql_host, mysql_port, mysql_user, mysql_password, mysql_db):
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_db = mysql_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mysql_host=crawler.settings.get('MYSQL_HOST'),
            mysql_port=crawler.settings.get('MYSQL_PORT'),
            mysql_user=crawler.settings.get('MYSQL_USER'),
            mysql_password=crawler.settings.get('MYSQL_PASSWORD'),
            mysql_db=crawler.settings.get('MYSQL_DB')
        )

    def open_spider(self, spider):
        try:
            self.connection = mysql.connector.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user=self.mysql_user,
                password=self.mysql_password,
                database=self.mysql_db
            )
            self.cursor = self.connection.cursor()

            # 创建表
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS baidu_hotsearch (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ranking INT NOT NULL,
                title VARCHAR(255) NOT NULL,
                link VARCHAR(512),
                hot_index VARCHAR(50),
                create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS baidu_novel (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ranking INT NOT NULL,
                title VARCHAR(255) NOT NULL,
                link VARCHAR(512),
                hot_index VARCHAR(50),
                cover_url VARCHAR(512),
                create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS baidu_movie (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ranking INT NOT NULL,
                title VARCHAR(255) NOT NULL,
                link VARCHAR(512),
                hot_index VARCHAR(50),
                movie_type VARCHAR(100),
                actors VARCHAR(255),
                description TEXT,
                cover_url VARCHAR(512),
                create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            self.connection.commit()
        except Error as e:
            spider.logger.error(f"MySQL连接错误: {e}")

    def close_spider(self, spider):
        if hasattr(self, 'connection') and self.connection.is_connected():
            self.cursor.close()
            self.connection.close()

    def process_item(self, item, spider):
        try:
            adapter = ItemAdapter(item)
            type_ = adapter.get('type_')

            if type_ == "news":
                self.cursor.execute(
                    "INSERT INTO baidu_hotsearch (ranking, title, link, hot_index) VALUES (%s, %s, %s, %s)",
                    (adapter.get('ranking'), adapter.get('title'), adapter.get('link'), adapter.get('hot_index'))
                )
            elif type_ == "novel":
                self.cursor.execute(
                    "INSERT INTO baidu_novel (ranking, title, link, hot_index, cover_url) VALUES (%s, %s, %s, %s, %s)",
                    (adapter.get('ranking'), adapter.get('title'), adapter.get('link'),
                     adapter.get('hot_index'), adapter.get('cover_url'))
                )
            elif type_ == "movie":
                self.cursor.execute(
                    """INSERT INTO baidu_movie (ranking, title, link, hot_index, movie_type, actors, description, cover_url) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (adapter.get('ranking'), adapter.get('title'), adapter.get('link'),
                     adapter.get('hot_index'), adapter.get('movie_type'), adapter.get('actors'),
                     adapter.get('description'), adapter.get('cover_url'))
                )

            self.connection.commit()
        except Error as e:
            spider.logger.error(f"MySQL插入错误: {e}")
        return item


class CsvPipeline:
    def open_spider(self, spider):
        self.news_file = open('baidu_hot_news.csv', 'w', newline='', encoding='utf-8-sig')
        self.news_writer = csv.writer(self.news_file)
        self.news_writer.writerow(['排名', '标题', '链接', '热度'])

        self.novel_file = open('baidu_hot_novel.csv', 'w', newline='', encoding='utf-8-sig')
        self.novel_writer = csv.writer(self.novel_file)
        self.novel_writer.writerow(['排名', '标题', '链接', '热度', '封面链接'])

        self.movie_file = open('baidu_hot_movie.csv', 'w', newline='', encoding='utf-8-sig')
        self.movie_writer = csv.writer(self.movie_file)
        self.movie_writer.writerow(['排名', '标题', '链接', '热度', '电影类型', '演员', '描述', '封面链接'])

    def close_spider(self, spider):
        self.news_file.close()
        self.novel_file.close()
        self.movie_file.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        type_ = adapter.get('type_')

        if type_ == "news":
            self.news_writer.writerow([
                adapter.get('ranking'), adapter.get('title'),
                adapter.get('link'), adapter.get('hot_index')
            ])
        elif type_ == "novel":
            self.novel_writer.writerow([
                adapter.get('ranking'), adapter.get('title'),
                adapter.get('link'), adapter.get('hot_index'),
                adapter.get('cover_url')
            ])
        elif type_ == "movie":
            self.movie_writer.writerow([
                adapter.get('ranking'), adapter.get('title'),
                adapter.get('link'), adapter.get('hot_index'),
                adapter.get('movie_type'), adapter.get('actors'),
                adapter.get('description'), adapter.get('cover_url')
            ])
        return item