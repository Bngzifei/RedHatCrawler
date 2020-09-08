# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import sys
import os
import time
import requests
from lxml import etree
from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import ElementNotInteractableException
import pymongo
import gevent
import asyncio
import aiohttp

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(BASE_DIR))

from redhat_crawler import constants
from redhat_crawler.logger import Logger
from redhat_crawler.utils import time_it, retry, dict2str
from redhat_crawler import parse_config

RedHatSpiderLogPath = os.path.join(os.path.dirname(os.getcwd()), "log")

if not os.path.exists(RedHatSpiderLogPath):
    os.makedirs(RedHatSpiderLogPath)

logger = Logger(log_name=os.path.join(RedHatSpiderLogPath, r"RedHatSpider.log"),
                log_level=1, logger="RedHatSpider").get_log()


class RedHatCrawler:
    """增量式RedHat爬虫"""

    def __init__(self, login_url, username, password):
        chrome_options = webdriver.ChromeOptions()
        # 静默模式
        # chrome_options.add_argument('--headless')
        # 解决 ERROR:browser_switcher_service.cc(238) 报错,添加下面的试用选项
        # chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--incognito')
        chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument('--hide-scrollbars')
        chrome_options.add_argument('user-agent={useragent}'.format(useragent=constants.USER_AGENT))
        self.driver = webdriver.Chrome(options=chrome_options)
        self.rhel7_base_url = constants.RHEL7_URL
        self.rhel8_base_url = constants.RHEL8_URL
        self.login_url = login_url
        self.username = username
        self.password = password
        self.failed_urls = list()
        # 是否首次爬取
        self.is_first = parse_config.get_config()
        # 创建客户端
        self.client = pymongo.MongoClient(host='localhost', port=27017)
        self.ver_nos = list()
        self.urls = list()

    def login_red_website(self):
        """登录red网站"""
        try:
            self.driver.get(self.login_url)
        except Exception as e:
            raise e
        logger.info(f"login_title: {self.driver.title}")
        self.driver.implicitly_wait(30)
        try:
            # 输入用户名
            self.driver.find_element_by_xpath("//div[@class='field']/input[@id='username']").send_keys(self.username)
            self.driver.find_element_by_xpath(
                '//div[@class="centered form-buttons"]/button[@class="centered button heavy-cta"]').click()
            time.sleep(2)

            # 输入密码
            self.driver.find_element_by_xpath("//div[@id='passwordWrapper']/input[@id='password']").send_keys(
                self.password)
            self.driver.find_element_by_xpath("//div[@id='kc-form-buttons']//input[@id='kc-login']").click()
            time.sleep(5)
        except ElementNotInteractableException as e:
            raise e

    # @retry(reNum=5)
    def get_all_rhel_urls(self, url):
        """获取所有下载链接"""
        try:
            self.driver.get(url)
        except Exception as e:
            logger.error(e)
        time.sleep(8)
        target_objs = self.driver.find_elements_by_xpath('//div[@class="option pull-left"][2]/select[@id="evr"]/option')
        version2urls = [{obj.get_attribute("text"): obj.get_attribute("value")} for obj in target_objs]
        if not version2urls:
            self.get_all_rhel_urls(url)

        return version2urls

    @retry(reNum=5)
    def get_target_page_cookie(self, url):
        """获取目标网页的cookie"""
        try:
            self.driver.get(url)
        except Exception as e:
            logger.error(e)
        rh_jwt = self.driver.get_cookie("rh_jwt")
        session = self.driver.get_cookie("_redhat_downloads_session")
        if all([rh_jwt, session]):
            logger.info(f"jwt: {rh_jwt}")
            logger.info(f"session: {session}")
            rh_jwt_value = rh_jwt["value"]
            session_value = session["value"]
            time.sleep(5)
            cookies = {
                "rh_user": "rd.sangfor|rd|P|",
                "rh_locale": "zh_CN",
                "rh_user_id": "51768269",
                "rh_sso_session": "1",
                "rh_jwt": rh_jwt_value,
                "_redhat_downloads_session": session_value
            }
            cookie_str = dict2str(cookies)
            return cookie_str
        else:
            logger.info(f"{url} 链接获取cookie失败,请重新获取")
            self.failed_urls.append(url)

    async def async_get(self, url, headers=None):
        """异步请求"""
        # 注意下面必须使用  async with 进行申明
        async with aiohttp.ClientSession(headers=headers) as session:
            response = await session.get(url)
            result = await response.text()
            return result

    @retry(reNum=5)
    async def save_target_data(self, cookie, target_url, filename):
        """保存数据"""
        headers = {
            "User-Agent": constants.USER_AGENT,
            "Cookie": cookie,
        }
        try:
            wb_data = await self.async_get(target_url, headers=headers)
        except Exception as e:
            logger.info(e)
        else:
            html = etree.HTML(wb_data)
            need_data = html.xpath('//div[@class="changelog"]//text()')
            logger.info("first element:{element}".format(element=need_data[0]))

            if need_data:
                try:
                    with open(filename, "w", encoding="utf-8", errors="ignore") as fp:
                        for data in need_data:
                            fp.write(data)
                except Exception as e:
                    logger.info(e)

    def save_url_to_mongodb(self, items, rhel_ver):
        """将url保存至mongodb"""
        logger.info(f"pymongo version: {pymongo.version}")
        # 指定数据库,如果没有则会自动创建
        db = self.client.redhat
        if rhel_ver == "rhel8":
            # 集合:就是数据表的概念
            collection = db.centos8_table
        else:
            # 有,修改,没有,创建
            collection = db.centos7_table

        datas = list()
        for ver_no, url in items:
            # mongo会自动创建id, 不需要自己创建
            # url_id = uuid.uuid4().__str__()
            data = {
                # "id": url_id,
                "ver_no": ver_no,
                "url": url,
            }
            datas.append(data)
        try:
            collection.insert_many(datas)
        except TypeError as ex:
            logger.error(ex)

    def query_url_by_kw(self, url, ver_no):
        """根据关键字查询mongodb中的url"""
        # 指定数据库,如果没有则会自动创建
        db = self.client.redhat
        collection = db.centos8_table
        item = {
            "url": url,
            "ver_no": ver_no
        }
        db_data = collection.find_one(item)
        return db_data

    def query_all_db_objs(self, rhel_ver):
        """查询所有的mongodb对象"""
        try:
            db = self.client.redhat
            if rhel_ver == "rhel8":
                collection = db.centos8_table
            else:
                collection = db.centos7_table
            db_objs = collection.find()
        except Exception as ex:
            logger.error(ex)
        else:
            return db_objs

    def query_all_ver_nos(self, rhel_ver):
        """查询所有ver_no"""
        db_objs = self.query_all_db_objs(rhel_ver)
        ver_nos = list()
        for obj in db_objs:
            ver_no = [i for i in obj.values()][1]
            ver_nos.append(ver_no)
        return ver_nos

    def get_rhel_urls(self, rhel_ver):
        """获取rhel所有url"""

        # 先登录,登录后携带cookie进行抓取数据
        self.login_red_website()
        if rhel_ver == "rhel8":
            version2urls = self.get_all_rhel_urls(self.rhel8_base_url)
        else:
            version2urls = self.get_all_rhel_urls(self.rhel7_base_url)

        for item in version2urls:
            url_suffix = [i for i in item.values()][0]
            ver_no = [i for i in item.keys()][0]
            url = "".join([constants.REDHAT_DOMAIN, url_suffix])
            self.ver_nos.append(ver_no)
            self.urls.append(url)

        logger.info(self.ver_nos)

    def craw_data_tasks(self, items, save_path):
        """抓取并保存数据"""
        tasks = list()
        for ver_no, url in items:
            # 非首次爬
            # 将url和mongodb中的进行对比
            # db_data = self.query_url_by_kw(url, ver_no)
            # if not db_data:
            # 说明这一条url是最新的,只爬取这一条
            # 处理的不好,使用集合进行去重,差集的处理更好
            logger.info("===>>>开始爬取{ver_no}...".format(ver_no=ver_no))
            cookie = self.get_target_page_cookie(url)
            filename = "".join([save_path, "\\", "changelog-", str(constants.TODAY), "-", ver_no, ".txt"])
            task = asyncio.ensure_future(self.save_target_data(cookie, url, filename))
            logger.info("===>>>{ver_no}更新日志已保存".format(ver_no=ver_no))
            tasks.append(task)

        return tasks

    def craw_sole_url_data(self, ver_no, url, save_path):
        """爬取单条url数据"""
        logger.info("===>>>开始爬取{ver_no}...".format(ver_no=ver_no))
        cookie = self.get_target_page_cookie(url)
        filename = "".join([save_path, "\\", "changelog-", str(constants.TODAY), "-", ver_no, ".txt"])
        self.save_target_data(cookie, url, filename)
        logger.info("===>>>{ver_no}更新日志已保存".format(ver_no=ver_no))
        time.sleep(5)

    def async_craw_data(self, items, save_path):
        """异步抓取并保存数据"""
        events = list()
        for ver_no, url in items:
            gl_obj = gevent.spawn(self.craw_sole_url_data(ver_no, url, save_path))
            events.append(gl_obj)

        return events

    def get_latest_rhel_data(self, items, save_path):
        """爬取最新的"""
        self.craw_data_tasks(items, save_path)

    def get_all_rhel_data(self, rhel_ver):
        """爬取所有rhel的数据"""
        if rhel_ver == "rhel8":
            save_path = constants.RHEL8_STORAGE_DIR
        else:
            save_path = constants.RHEL7_STORAGE_DIR

        if not os.path.exists(save_path):
            os.makedirs(save_path)

        # 获取爬取链接
        self.get_rhel_urls(rhel_ver)
        # 取出网页爬取的真实url-ver_no
        objs = [(ver_no, url) for ver_no, url in zip(self.ver_nos, self.urls)]
        ver_nos = list()
        for obj in objs:
            ver_no = obj[0]
            ver_nos.append(ver_no)

        if self.is_first == str(0):
            # 这里的问题:查询的时候应该根据指定的版本号去对应的表查询
            db_ver_nos = self.query_all_ver_nos(rhel_ver)
            logger.info(f"当前网页的版本号: {ver_nos}")
            logger.info(f"数据库中已有版本号: {db_ver_nos}")
            # 取差集
            latest_ver_nos = list(set(ver_nos) - set(db_ver_nos))
            logger.info(f"最新的版本号: {latest_ver_nos}")
            latest_items = [obj for obj in objs if obj[0] in latest_ver_nos]
            logger.info(objs)
            logger.info(f"最新的版本号和url链接: {latest_items}")
            # 后续爬取的就是差集部分这些
            # 然后爬完也要把数据存入mongodb
            self.get_latest_rhel_data(latest_items, save_path)
            self.save_url_to_mongodb(latest_items, rhel_ver)
            return

        zip_objs = zip(self.ver_nos, self.urls)
        # 异步方式
        tasks = self.craw_data_tasks(zip_objs, save_path)
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(asyncio.wait(tasks))
        # 注意这个driver退出的位置
        self.driver.quit()
        # 第一次爬完,将url存入mongodb
        self.save_url_to_mongodb(zip(self.ver_nos, self.urls), rhel_ver)
        # 标识量置为False,但是再次执行脚本,这里还是会为True, 无法持久化保存
        # 所以:办法 1、存入数据库这个变量 2、存入配置文件
        self.is_first = "0"
        parse_config.update_config(self.is_first)


@time_it
def main():
    red_spider = RedHatCrawler(constants.LOGIN_URL, constants.USERNAME, constants.PASSWORD)
    version = parse_config.get_rhel_version()
    red_spider.get_all_rhel_data(version)


if __name__ == '__main__':
    start_time = parse_config.get_current_time()
    parse_config.update_start_crawl_time(start_time)
    main()
    end_time = parse_config.get_current_time()
    parse_config.update_end_crawl_time(end_time)
