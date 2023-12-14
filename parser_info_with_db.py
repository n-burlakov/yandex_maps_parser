import os
import json
import random
import logging
import coloredlogs
import os
import argparse
from datetime import datetime
import asyncio
import aiofiles
from time import sleep

import requests
from PIL import Image
from io import BytesIO

import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, ElementNotInteractableException, \
    MoveTargetOutOfBoundsException, InvalidSessionIdException, WebDriverException

from soup_parser import SoupContentParser
# from utils import json_pattern
from utils.wutil import get_webdriver, getLogger, get_url
from add_to_db import PostgresUtil  # connect, add_row_to_db

coloredlogs.install(level='INFO')
module_logger = getLogger("parse_info_company")


def into_json(name, city, address, logo, website, ypage, rating):
    """ Шаблон файла OUTPUT.json"""

    data_grabbed = {
        "company_name": name,
        "city": city,
        "address": address,
        "company_url": website,
        "company_rating": rating,
        "logo": logo,
        "yandex_url": ypage,
    }
    return data_grabbed


class Parser:

    def __init__(self, step: int = 8, type_org: str = None, city: str = None):
        """

        :param step: amount of parsing threads;
        :param max_errors: maximum errors when scroll page down;
        :param type_org: type of organization.
        """
        self.sentinel = object()
        self.step = step
        self.org_id = 0
        self.type_org = type_org
        self.df = pd.DataFrame()
        self.db_utils = PostgresUtil()
        self.city = city
        self.soup_parser = SoupContentParser()
        self.connect = None

    async def parse_reviews(self, driver, link_to_reviews, amount_of_revs, company_id):
        """
            Get reviews from link page.
        :param driver: selenium webdriver of page;
        :param link_to_reviews: tail of reviews link;
        :param amount_of_revs: amount of reviews on this page
        :param company_id: company_id
        :return: list of dicts with review data.
        """
        # driver.get('https://yandex.ru' + link_to_reviews)
        driver = await get_url(driver, 'https://yandex.ru' + link_to_reviews)
        main_reviews_list = []
        count, error_counts = 0, 0
        max_errors = 10
        errors = 0

        slider = driver.find_element(By.CLASS_NAME, 'scroll__scrollbar-thumb')
        while max_errors > errors:
            try:
                ActionChains(driver).click_and_hold(slider).move_by_offset(0, int(100 / errors)).release().perform()
                if amount_of_revs - (amount_of_revs * 0.05) <= len(
                        driver.find_elements(By.CLASS_NAME, "business-reviews-card-view__review")):
                    break
                await asyncio.sleep(random.uniform(0.9, 1.8))
            except StaleElementReferenceException:
                slider = driver.find_element(By.CLASS_NAME, 'scroll__scrollbar-thumb')
                await asyncio.sleep(random.uniform(0.8, 1.2))
            except Exception:
                errors += 1
                module_logger.error(f"amount of errors: " + str(errors))
                await asyncio.sleep(random.uniform(0.5, 1))
            try:
                if (max_errors - 1) <= errors and slider.location['y'] + 5 < int(
                        driver.execute_script("return document.documentElement.scrollHeight")):
                    r = int(driver.execute_script("return document.documentElement.scrollHeight")) - slider.location[
                        'y']
                    try:
                        ActionChains(driver).click_and_hold(slider).move_by_offset(0, r - 5).release().perform()
                    except MoveTargetOutOfBoundsException:
                        break
                    except ElementNotInteractableException as exc:
                        module_logger.error(str(exc))
                        pass
                    await asyncio.sleep(random.uniform(0.8, 1))
                    errors -= 5
                    error_counts += 1
                    if error_counts > 5:
                        break
            except InvalidSessionIdException:
                pass

        soup = BeautifulSoup(driver.page_source, "lxml")
        reviews_list = soup.find_all("div", {"class", "business-reviews-card-view__review"})
        module_logger.info(
            f"{'-' * 100} \n Amount of reviews is {len(reviews_list)}, has started parse! \n {'-' * 100}")
        for rev in reviews_list:
            try:
                name_author = rev.find("div", {"class": "business-review-view__author"}).find("span").text.replace("'",
                                                                                                                   "`")
                try:
                    rating = len(
                        rev.find("div", {"class": "business-rating-badge-view__stars"}).find_all("span",
                                                                                                 {"class": "_full"}))
                except Exception as exc:
                    if "'NoneType' object has no attribute 'find_all'" in exc:
                        module_logger.error(str(exc))
                        module_logger.info(
                            str(rev.find("div",
                                         {"class": "business-rating-badge-view__stars"})) + f'\n {"-------" * 30}')
                        rating = random.randint(2, 4)
                    else:
                        raise exc

                date_time = \
                    rev.find("span", {"class": "business-review-view__date"}).find("meta").get("content").replace('T',
                                                                                                                  ' ').split(
                        ".")[0]

                date = date_time.split(' ')[0]
                time = date_time.split(' ')[1]
                try:
                    review_text = rev.find("span", {"class": "business-review-view__body-text"}).text.replace("'", "`")

                except AttributeError:
                    review_text = None
                main_reviews_list.append({"author_name": name_author, "rating": rating, "city": self.city, "date": date,
                                          "time": time, "text": review_text, "company_id": company_id})
            except Exception as exc:
                module_logger.error(exc)
        return main_reviews_list

    async def save_image(self, path: str, image: memoryview) -> None:
        async with aiofiles.open(path, "wb") as file:
            await file.write(image)

    async def save_logo_img(self, img_url, name):
        """
            Saving logo image.
        :param img_url: url to logo image;
        :param name: name of company + address;
        :return: path to image.
        """
        directory = f'logos/{self.type_org}/'
        if not os.path.exists(directory):
            os.makedirs(directory)
        try:
            img_name = directory + self.type_org + "_" + name + ".png"
            img = Image.open(requests.get(img_url, stream=True).raw)
            buffer = BytesIO()
            img.save(buffer, format="PNG")
            await self.save_image(img_name, buffer.getbuffer())
        except Exception as exc:
            module_logger.error(f'error during saving image {exc}')
            img_name = ''
        return img_name

    async def concat_dataframe(self, temp_df):
        self.df = pd.concat([self.df, temp_df])
        if len(self.df) % 500 == 0:
            self.df.to_csv(f'result_output/{self.type_org}_outputs.csv')
            module_logger.info(
                f"{'-' * 100} \n Saving dataframe with info and length {len(self.df)}! \n {'-' * 100}")

    async def parse_data(self, driver, connect):
        """
            Parse page info method.
        :param driver: selenium webdriver;
        :return: dataframe with information about company and reviews.
        """
        # try:
        soup = BeautifulSoup(driver.page_source, "lxml")
        amount_of_reviews, link_to_reviews = self.soup_parser.amount_reviws(soup)
        if amount_of_reviews >= 10:
            name = self.soup_parser.get_name(soup)
            address = self.soup_parser.get_address(soup)
            company = name + ' ' + address
            website = self.soup_parser.get_website(soup)
            ypage = driver.current_url
            rating = self.soup_parser.get_rating(soup)
            logo = await self.save_logo_img(img_url=self.soup_parser.get_logo(soup), name=name)
            output = into_json(name=name, city=self.city, address=address, logo=logo, website=website,
                               ypage=ypage, rating=rating)

            company_id, repeat = await self.db_utils.add_row_to_db(connection=connect, table_name='CompaniesInfo',
                                                                   info_list=[output])
            if not repeat:
                reviews_dict = await self.parse_reviews(driver=driver, link_to_reviews=link_to_reviews,
                                                        amount_of_revs=amount_of_reviews, company_id=company_id)
                await self.db_utils.add_row_to_db(connection=connect, table_name='Reviews', info_list=reviews_dict)
                reviews = {company: reviews_dict}

                output['reviews'] = reviews
                temp_df = pd.DataFrame().from_dict(output)

                await self.concat_dataframe(temp_df)
            else:
                module_logger.info(f"this company was parsed and added to db with id {company_id}")
        # except Exception as exc:
        #     module_logger.error(str(exc))

    async def get_url(self, url):
        """
            Get webdriver and url to company on Yandex maps
        :param url: link to company;
        """
        module_logger.info(f"Get for parsing {url}")
        driver = await get_webdriver()
        driver = await get_url(driver, url)
        try:
            connect = await self.db_utils.connect(
                {"database": "yandex", "host": "localhost", "user": "postgres", "password": "12345678"})
            ypage = driver.current_url
            exists = await self.db_utils.company_in_db(connect, 'CompaniesInfo', ypage)
            if exists:
                module_logger.info("this company already parsed!")
            else:
                await self.parse_data(driver, connect)
        except WebDriverException as exc:
            module_logger.error(str(exc))
            self.df.to_csv(f'result_output/{self.type_org}_{self.city}_outputs.csv')
        except Exception as error:
            module_logger.error(str(error))
            pass
        finally:
            try:
                driver.close()
                driver.quit()
            except:
                pass

    async def main(self, all_hrefs):
        prev_num = 0
        for num in range(0, len(all_hrefs) + 1, self.step):
            if num + self.step > len(all_hrefs):
                num = len(all_hrefs)
            hrefs = all_hrefs[prev_num:num]
            prev_num = num
            if not hrefs:
                continue
            tasks = [self.get_url(url) for url in hrefs]
            for task in asyncio.as_completed(tasks):
                await task
        self.df.to_csv(f'result_output/{self.type_org}_{self.city}_outputs.csv')
        print(f"Page with reviews for {self.type_org} was collected!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("type_org", help="organization type")
    args = parser.parse_args()
    type_org = args.type_org

    # type_org = "pet_shop"
    all_hrefs = []
    files = os.listdir(f'links/{type_org}')
    for file in files:
        with open(f'links/{type_org}/{file}', 'r', encoding='utf-8') as f:
            hrefs = json.load(f)['1']
            all_hrefs += hrefs
    all_hrefs = list(set(all_hrefs))
    parser = Parser(type_org=type_org)
    asyncio.run(parser.main(all_hrefs[:3]))
