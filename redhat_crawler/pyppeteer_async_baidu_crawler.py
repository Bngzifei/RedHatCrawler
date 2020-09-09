# -*- coding: utf-8 -*-
import asyncio

from pyppeteer import launch


async def baidu_spider(url, title):
    print(f"{title} task start...")
    browser = await launch()
    page = await browser.newPage()
    await page.goto(url, {"timeout": 0})
    await page.screenshot({"path": f"{title}.png"})
    await browser.close()
    print(f"{title} task end...")


urls = ["https://www.baidu.com", "https://www.jd.com", "https://www.qq.com"]
tasks = [asyncio.ensure_future(baidu_spider(url, url.split(".")[1])) for url in urls]
event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(asyncio.wait(tasks))
