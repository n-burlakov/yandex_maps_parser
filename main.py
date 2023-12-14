from link_parser import LinksCollector
from info_parser import Parser
from parser_info_with_db import Parser as ParserDB
import asyncio
import time
from utils.constants import type_org_mapping
from utils import wutil as wu

import json
import os
import ast
import shutil
import argparse
import timeit
import logging
import coloredlogs

coloredlogs.install(level='INFO')
module_logger = wu.getLogger("main_parse_logs")


def decor_timer(f):
    async def _fn(**kwargs):
        starttime = time.time()
        await f(**kwargs)
        print(
            "Function run time is ",
            (time.time() - starttime), "s",
        )

    return _fn


async def run_parser(type_org: str = None, cities: list = None, step: int = 5):
    for city in cities:
        links_task = LinksCollector().run(city=city, type_org_ru=type_org_mapping[type_org], type_org=type_org)
        asyncio.run(links_task)
        info_task = Parser(type_org=type_org).main(all_hrefs=all_links)
        asyncio.run(info_task)


def get_all_links(type_org, city):
    all_links = []
    try:
        files = os.listdir(f'links/{type_org}')
    except:
        os.makedirs(f'links/{type_org}')
    for file in files:
        if '.txt' in file and city in file:
            with open(f'links/{type_org}/{file}', 'r', encoding='utf-8') as f:
                hrefs = f.read().replace("'", "").replace('[', '').replace(']', '').split(', ')
                all_links += hrefs
                file_name = f'links/{type_org}/{file}'
    all_links = list(set(all_links))
    module_logger.info("Amount of links for parsing is: " + str(len(all_links)))
    return all_links, file_name


@decor_timer
async def run_all_parsers(cities: list = None, step: int = 5):
    for dir in ["links", "done_links"]:
        if not os.path.exists(dir):
            os.makedirs(dir)

    while cities:
        city = cities.pop(0)

        links_tasks = list()
        for type_org, type_org_ru in type_org_mapping.items():
            while True:
                try:
                    if not os.path.exists(f'links/{type_org}/{city} {type_org_ru}.txt'):
                        print(type_org_ru)
                        link_obj = LinksCollector()
                        links_tasks.append(
                            asyncio.create_task(
                                link_obj.get_data(city=city, type_org_ru=type_org_ru, type_org=type_org)))
                        if len(links_tasks) >= step or list(type_org_mapping.keys())[-1] == type_org:
                            await asyncio.gather(*links_tasks)
                            links_tasks.clear()
                    break
                except Exception as exc:
                    module_logger.error(">> Appear error while running parse organisation links : "+str(exc))
                    time.sleep(60)

        for type_org, type_org_ru in type_org_mapping.items():
            pars_list_task = list()
            all_links, file_name_from = get_all_links(type_org, city)
            while True:
                try:
                    # pars_obj = Parser(step=step, type_org=type_org, city=city)
                    pars_obj = ParserDB(step=step, type_org=type_org, city=city)
                    pars_list_task.append(pars_obj.main(all_hrefs=all_links))
                    if len(pars_list_task) >= step or list(type_org_mapping.keys())[-1] == type_org:
                        await asyncio.gather(*pars_list_task)
                        pars_list_task.clear()
                    shutil.move(file_name_from, f"done_links/{city}_{type_org_ru}_parsed.txt")
                    break
                except Exception as exc:
                    module_logger.error(">> Appear error while running parse info: "+str(exc))
                    time.sleep(60)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--run_all", action='store_true', help="run parse for all rubrics")
    parser.add_argument("--run_rubric", type=str, help="organization type as: pet_shop or realty")
    parser.add_argument("--threads", type=int, default=8, help="amount of parsing threads, default has 5.")
    parser.add_argument("--cities", type=str, default="Москва",
                        help="String of cities which will be parsed, separated comma with space - ', '. For example: Москва, Санкт-Петербург")

    args = parser.parse_args()
    cities = args.cities.split(', ')
    print(cities)
    if args.run_rubric:
        type_org = args.run_rubric(cities=cities)
        run_parser(type_org, cities, step=args.threads)

    elif args.run_all:
        asyncio.run(run_all_parsers(cities=cities, step=args.threads))

    else:
        raise ValueError(f'Do not know action {args.act}')
