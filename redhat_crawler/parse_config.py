# -*- coding: utf-8 -*-
import os
import configparser
from configobj import ConfigObj
from datetime import datetime

# 获取当前文件所在目录的上一级目录，即项目所在目录 E:\Crawler
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def parse_config():
    """解析配置文件"""
    cf = configparser.ConfigParser()
    # 拼接得到config.ini文件的路径，直接使用
    cf.read(os.path.join(BASE_DIR, "conf.ini"))
    # 获取文件中所有的section(一个配置文件中可以有多个配置，
    # 如数据库相关的配置，邮箱相关的配置，
    # 每个section由[]包裹，即[section])，并以列表的形式返回
    secs = cf.sections()
    print(secs)
    # 获取某个section名为Mysql-Database所对应的键
    options = cf.options("Mysql-Database")
    print(options)
    # 获取section名为Mysql-Database所对应的全部键值对
    items = cf.items("Mysql-Database")
    print(items)
    # 获取[Mysql-Database]中host对应的值
    host = cf.get("Mysql-Database", "host")
    print(host)


def get_config():
    """读取配置"""
    # *** 配置文件预处理 *** #
    config = ConfigObj("conf.ini", encoding='UTF8')

    # *** 读配置文件 *** #
    # print(config['txtB'])
    is_first = config['IsFirst']['is_first']
    return is_first


def update_config(is_first):
    """修改配置"""

    # *** 配置文件预处理 *** #
    config = ConfigObj("conf.ini", encoding='UTF8')

    # *** 读配置文件 *** #
    # print(config['txtB'])
    # print(config['txtB']['name'])

    # *** 修改配置文件 *** #
    config['IsFirst']['is_first'] = is_first
    config.write()

    # *** 添加section *** #
    # config['txtC'] = {}
    # config['txtC']['index0'] = "wanyu00"
    # config.write()


def update_start_crawl_time(current_time):
    """更新开始爬取时间"""
    # *** 配置文件预处理 *** #
    config = ConfigObj("conf.ini", encoding='UTF8')
    # *** 修改配置文件 *** #
    config['CrawlTime']['start_time'] = current_time
    config.write()


def update_end_crawl_time(current_time):
    """更新爬取结束时间"""

    # *** 配置文件预处理 *** #
    config = ConfigObj("conf.ini", encoding='UTF8')
    # *** 修改配置文件 *** #
    config['CrawlTime']['end_time'] = current_time
    config.write()


def get_current_time():
    """获取当前时间"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return current_time


def get_rhel_version():
    """获取版本"""

    # 预处理
    config = ConfigObj("conf.ini", encoding='utf-8')

    # 读取版本
    version = config['RhelVersion']['rhel_version']
    return version
