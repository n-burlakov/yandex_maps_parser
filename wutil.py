from typing import Any
import asyncio
from logging import handlers
import logging
import os
import random

from seleniumwire import webdriver as webdriver_wire
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from fake_useragent import UserAgent


def check_file_exists(file_path):
    if not os.path.exists(file_path):
        os.makedirs(file_path)


async def get_url(driver: Any = None, url: str = None) -> Any:
    driver.get(url)
    driver.maximize_window()
    await asyncio.sleep(random.uniform(1, 2))
    return driver


async def get_webdriver(proxy: str = None) -> Any:
    """
        Create webdriver object with options and proxy secure

    :param proxy: proxy name;
    :return: webdriver object.
    """
    useragent = UserAgent()

    options = webdriver_wire.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    if proxy:
        options.add_argument(f"--proxy-server=https://{proxy}")
    options.add_argument('--headless=new')  # turn off opening browser window
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument(f"user-agent={useragent.random}")
    # options.set_capability("goog:loggingPrefs", {'browser': 'ALL'})

    return webdriver_wire.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)


def getLogger(name=None, level=logging.DEBUG):
    logger = logging.getLogger(name)
    name = 'root' if name is None else name
    if not os.path.exists(os.path.expanduser('~/logs')):
        os.makedirs(os.path.expanduser('~/logs'))
    logger.setLevel(level)
    logger.propagate = True
    logger_Handler = handlers.TimedRotatingFileHandler(filename=f"{os.path.expanduser('~/logs')}/{name}.log")
    logger_Formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    logger_Handler.setFormatter(logger_Formatter)
    logger.addHandler(logger_Handler)
    return logger
