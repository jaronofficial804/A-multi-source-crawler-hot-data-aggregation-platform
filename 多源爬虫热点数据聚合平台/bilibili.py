import time, csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import mysql.connector


class BilibiliSpider:
    def __init__(self):
        # B站热门视频页面
        self.url = "https://www.bilibili.com/v/popular/all"
        # 启动 Chrome 浏览器
        self.driver = webdriver.Chrome()
        # 视频计数器
        self.video_count = 0
        # CSV 文件名
        self.csv_file = 'bilibili_hot.csv'
        # 数据字段名
        self.fieldnames = ['title', 'link', 'cover', 'up_name', 'play_count', 'danmaku_count', 'timestamp']
        # 初始化 CSV 文件
        self._init_csv()
        # 初始化 MySQL 数据库连接与表
        self._init_mysql()

    def _init_csv(self):
        """创建 CSV 文件并写入表头"""
        with open(self.csv_file, 'w', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=self.fieldnames).writeheader()

    def _init_mysql(self):
        """连接数据库，创建数据库和数据表"""
        self.db = mysql.connector.connect(host="localhost", user="root", password="root123", charset='utf8mb4')
        cursor = self.db.cursor()
        # 创建数据库（如果不存在）
        cursor.execute("CREATE DATABASE IF NOT EXISTS bilibili_database CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        self.db.database = 'bilibili_database'
        # 创建数据表（如果不存在）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bilibili_videos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255),
                link VARCHAR(255),
                cover VARCHAR(255),
                up_name VARCHAR(255),
                play_count VARCHAR(255),
                danmaku_count VARCHAR(255),
                timestamp VARCHAR(255)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        self.db.commit()
        self.cursor = cursor

    def scroll_to_bottom(self):
        """滚动页面至底部，加载所有视频内容"""
        last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            time.sleep(3)  # 等待页面加载
            new_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def parse_card(self, card):
        """解析单个视频卡片元素"""
        try:
            return {
                'title': card.find_element(By.CLASS_NAME, 'video-name').get_attribute('title'),  # 视频标题
                'link': card.find_element(By.TAG_NAME, 'a').get_attribute('href'),               # 视频链接
                'cover': card.find_element(By.CLASS_NAME, 'cover-picture__image').get_attribute('src') or '',  # 封面图链接
                'up_name': card.find_element(By.CLASS_NAME, 'up-name__text').text,               # UP主名称
                'play_count': card.find_element(By.XPATH, './/span[contains(@class, "play-text")]').text.strip(),  # 播放量
                'danmaku_count': card.find_element(By.XPATH, './/span[contains(@class, "like-text")]').text.strip(), # 弹幕/点赞数
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())                # 当前时间戳
            }
        except Exception as e:
            print(f"解析失败: {e}")
            return None

    def save_data(self, data):
        """将数据保存到 CSV 文件和 MySQL 数据库"""
        # 写入 CSV 文件
        with open(self.csv_file, 'a', newline='', encoding='utf-8-sig') as f:
            csv.DictWriter(f, fieldnames=self.fieldnames).writerow(data)
        # 插入数据库
        try:
            self.cursor.execute("""
                INSERT INTO bilibili_videos (title, link, cover, up_name, play_count, danmaku_count, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, tuple(data.values()))
            self.db.commit()
        except mysql.connector.Error as err:
            print(f"MySQL 错误: {err}")

    def run(self):
        """主程序：打开页面、滚动、解析并保存所有视频信息"""
        try:
            self.driver.get(self.url)
            # 等待页面加载出视频卡片
            WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, 'video-card')))
            print("开始滚动页面加载更多内容...")
            self.scroll_to_bottom()

            # 获取所有视频卡片元素
            cards = self.driver.find_elements(By.CLASS_NAME, 'video-card')
            print(f"共找到 {len(cards)} 个视频")

            # 逐个解析并保存
            for card in cards:
                data = self.parse_card(card)
                if data:
                    self.save_data(data)
                    self.video_count += 1
                    print(f"[{self.video_count}] {data['title']} - 播放: {data['play_count']} 弹幕: {data['danmaku_count']}")

        except Exception as e:
            print(f"运行出错: {e}")
        finally:
            # 清理资源
            self.driver.quit()
            self.cursor.close()
            self.db.close()
            print(f"爬取完成，共爬取 {self.video_count} 条视频信息")


# 程序入口
if __name__ == '__main__':
    BilibiliSpider().run()
