import requests
from bs4 import BeautifulSoup
import csv
import mysql.connector
from mysql.connector import Error
import threading
import queue
import time
import random

# 模拟不同浏览器的请求头
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36'
]
#获取随机请求头函数
def get_random_user_agent():
    return random.choice(user_agents)
#创建队列
url_queue = queue.Queue()
data_queue = queue.Queue()

# 百度热搜页加入队列
url_queue.put(("https://top.baidu.com/board?tab=realtime", "news"))
url_queue.put(("https://top.baidu.com/board?tab=novel", "novel"))
url_queue.put(("https://top.baidu.com/board?tab=movie", "movie"))

print_lock = threading.Lock()
# 爬取百度热搜数据的函数
def crawl_baidu_hot():
    while not url_queue.empty():
        url, type_ = url_queue.get()
        try:
            time.sleep(random.uniform(0.5, 1.2))
            headers = {'User-Agent': get_random_user_agent()}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.find_all('div', class_='category-wrap_iQLoo')

            for index, item in enumerate(items, 1):
                try:
                    title_tag = item.select_one('a.title_dIF3B div.c-single-text-ellipsis')
                    title = title_tag.text.strip() if title_tag else '无标题'
                    link_tag = item.select_one('a.title_dIF3B')
                    link = link_tag['href'] if link_tag else '无链接'
                    hot_index_tag = item.select_one('div.hot-index_1Bl1a')
                    hot_index = hot_index_tag.text.strip() if hot_index_tag else '无热度'
                    cover_img = item.select_one('a.img-wrapper_29V76 img')
                    cover_url = cover_img['src'] if cover_img else '无封面'

                    if type_ == "news":
                        data_queue.put((type_, index, title, link, hot_index))
                    elif type_ == "novel":
                        data_queue.put((type_, index, title, link, hot_index, cover_url))
                    elif type_ == "movie":
                        type_tag = item.select_one('div.intro_1l0wp')
                        movie_type = type_tag.text.replace('类型：', '').strip() if type_tag else '无类型'
                        actor_tag = item.find('div', class_='intro_1l0wp', string=lambda x: x and '演员：' in x)
                        actors = actor_tag.text.replace('演员：', '').strip() if actor_tag else '无演员信息'
                        desc_tag = item.select_one('div.desc_3CTjT')
                        description = desc_tag.text.strip() if desc_tag else '无描述'

                        data_queue.put(
                            (type_, index, title, link, hot_index, movie_type, actors, description, cover_url)
                        )
                except Exception as e:
                    with print_lock:
                        print(f"[{type_} 数据解析失败] {e}")
        except Exception as e:
            with print_lock:
                print(f"[{type_} 请求失败] {e}")
        finally:
            url_queue.task_done()

# 启动线程
threads = []
for _ in range(5):
    t = threading.Thread(target=crawl_baidu_hot)
    t.start()
    threads.append(t)

url_queue.join()

# 分类数据
hot_data, novel_data, movie_data = [], [], []
while not data_queue.empty():
    item = data_queue.get()
    type_ = item[0]
    if type_ == "news":
        hot_data.append(item[1:5])
    elif type_ == "novel":
        novel_data.append(item[1:])
    elif type_ == "movie":
        movie_data.append(item[1:])

# 写入数据库 & 读取写入CSV
try:
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root123',
        database='baidudatabase'
    )

    if connection.is_connected():
        cursor = connection.cursor()

        # 创建表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS baidu_hotsearch (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ranking INT NOT NULL,
            title VARCHAR(255) NOT NULL,
            link VARCHAR(512),
            hot_index VARCHAR(50),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("""
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
        cursor.execute("""
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

        # 插入数据
        cursor.executemany(
            "INSERT INTO baidu_hotsearch (ranking, title, link, hot_index) VALUES (%s, %s, %s, %s)",
            hot_data
        )
        cursor.executemany(
            "INSERT INTO baidu_novel (ranking, title, link, hot_index, cover_url) VALUES (%s, %s, %s, %s, %s)",
            novel_data
        )
        cursor.executemany(
            "INSERT INTO baidu_movie (ranking, title, link, hot_index, movie_type, actors, description, cover_url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            movie_data
        )

        connection.commit()

        # 写入CSV
        cursor.execute("SELECT ranking, title, link, hot_index FROM baidu_hotsearch")
        news_rows = cursor.fetchall()
        with open('baidu_hot_news.csv', 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['排名', '标题', '链接', '热度'])
            writer.writerows(news_rows)

        cursor.execute("SELECT ranking, title, link, hot_index, cover_url FROM baidu_novel")
        novel_rows = cursor.fetchall()
        with open('baidu_hot_novel.csv', 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['排名', '标题', '链接', '热度', '封面链接'])
            writer.writerows(novel_rows)

        cursor.execute("SELECT ranking, title, link, hot_index, movie_type, actors, description, cover_url FROM baidu_movie")
        movie_rows = cursor.fetchall()
        with open('baidu_hot_movie.csv', 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['排名', '标题', '链接', '热度', '电影类型', '演员', '描述', '封面链接'])
            writer.writerows(movie_rows)

        print(f"成功插入数据库并写入 CSV：{len(news_rows)} 条新闻，{len(novel_rows)} 条小说，{len(movie_rows)} 条电影")

except Error as e:
    print(f"数据库错误: {e}")
finally:
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL连接已关闭")
