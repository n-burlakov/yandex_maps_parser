import os
import argparse
import asyncio
import json
import logging, coloredlogs
import os
import random
from time import sleep

import aiofiles
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import MoveTargetOutOfBoundsException, WebDriverException
from utils.constants import ACCEPT_BUTTON, type_org_mapping
from utils.wutil import get_webdriver, getLogger, get_url

coloredlogs.install(level='INFO')
module_logger = getLogger("parse_links")
# root_logger = wu.getLogger()


class LinksCollector:

    def __init__(self,
                 link='https://yandex.ru/maps',
                 max_errors=10,
                 accept_button=ACCEPT_BUTTON,
                 accept=False,
                 proxy=None):
        """
            Collect links to organisations by type of searching
        :param link: link to Yandex maps;
        :param max_errors: maximum errors when scroll page down;
        :param accept_button: accept button for cookie;
        :param accept: accept to cookie;
        :param proxy: proxy for webdriver parsing;
        """
        self.slider = None
        self.proxy = proxy
        self.max_errors = max_errors
        self.link = link
        self.accept_button = accept_button
        self.accept = accept

    async def _open_page(self, request, driver):
        driver = await get_url(driver, self.link)
        WebDriverWait(driver, 200).until(EC.element_to_be_clickable(
            driver.find_element(By.CLASS_NAME, 'search-form-view__input').find_element(By.TAG_NAME,
                                                                                       "input"))).send_keys(request)
        await asyncio.sleep(random.uniform(0.4, 0.7))
        WebDriverWait(driver, 200).until(EC.element_to_be_clickable(
            driver.find_element(By.CLASS_NAME, 'small-search-form-view__button'))).click()
        # Нажимаем на кнопку поиска
        await asyncio.sleep(random.uniform(1.4, 2))

        if self.accept:
            # Соглашение куки
            flag = True
            count = 0
            while flag:
                try:
                    count += 1
                    await asyncio.sleep(3)
                    driver.find_element_by_xpath(self.accept_button).click()
                    flag = False
                except:
                    if count > 5:
                        driver.quit()
                        self._init_driver()
                        await self._open_page(request)
                    flag = True

    async def get_data(self, city, type_org_ru, type_org):
        """
            parse links to organizations
        :param type_org: selenium webdriver;
        :param city: city where will parse info;
        :param type_org_ru: type of org-on on Russion lang;
        :return: request message with list of links to organization.
        """
        driver = await get_webdriver(self.proxy)
        try:
            count, error_counts = 0, 0
            link_number = [0]
            max_errors = 10
            errors = 0
            organizations_hrefs = []

            request_msg = city + ' ' + type_org_ru
            await self._open_page(request_msg, driver)
            await asyncio.sleep(random.uniform(0.9, 2))
            slider = driver.find_element(By.CLASS_NAME, 'scroll__scrollbar-thumb')
            while max_errors > errors:
                try:
                    ActionChains(driver).click_and_hold(slider).move_by_offset(0, int(100 / errors)).release().perform()
                    count += 1
                    if count % 5 == 0:
                        if len(driver.find_elements(By.CLASS_NAME, 'search-snippet-view__link-overlay')) == link_number[-1]:
                            errors += 0.5
                        link_number.append(len(driver.find_elements(By.CLASS_NAME, 'search-snippet-view__link-overlay')))

                    await asyncio.sleep(random.uniform(0.9, 1.8))
                except StaleElementReferenceException:
                    slider = driver.find_element(By.CLASS_NAME, 'scroll__scrollbar-thumb')
                    await asyncio.sleep(random.uniform(0.8, 1.2))
                except Exception:
                    errors += 1
                    module_logger.error(f"amount of errors for {type_org_ru}: " + str(errors))
                    await asyncio.sleep(random.uniform(0.5, 1))
                if (max_errors - 1) <= errors and slider.location['y'] + 5 < int(driver.execute_script("return document.documentElement.scrollHeight")):
                    r = int(driver.execute_script("return document.documentElement.scrollHeight")) - slider.location['y']
                    try:
                        ActionChains(driver).click_and_hold(slider).move_by_offset(0, r - 7).release().perform()
                    except MoveTargetOutOfBoundsException:
                        break
                    await asyncio.sleep(random.uniform(0.8, 1))
                    errors -= 5
                    error_counts += 1
                    if error_counts > 5:
                        break

            slider_organizations_hrefs = driver.find_elements(By.CLASS_NAME, 'search-snippet-view__link-overlay')
            slider_organizations_hrefs = [href.get_attribute("href") for href in slider_organizations_hrefs]
            organizations_hrefs = list(set(organizations_hrefs + slider_organizations_hrefs))


            directory = f'links/{type_org}'
            if not os.path.exists(directory):
                os.makedirs(directory)
            async with aiofiles.open(f'{directory}/{request_msg}.txt', 'w') as file:
                await file.write(str(organizations_hrefs))
                await file.flush()
            module_logger.info(f"All links {len(organizations_hrefs)}, for {type_org_ru} in {city} was collected!")

            return organizations_hrefs
        except WebDriverException as exc:
            module_logger.error(str(exc))
        finally:
            driver.close()
            driver.quit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("type_org", help="organization type")
    args = parser.parse_args()
    type_org = args.type_org

    for type_org in ['pet_shop']:
        sleep(1)
        driver = get_webdriver()
        grabber = LinksCollector()
        grabber.run(city="Москва", district=None, type_org_ru=type_org_mapping[type_org], type_org=type_org)
