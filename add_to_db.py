import os
import time

import logging
import coloredlogs
import json
from typing import Any, Dict
import sys

import asyncio
import asyncpg
import psycopg2
from utils.wutil import getLogger
import pandas as pd
from gpt_refactor import change_review


coloredlogs.install(level='INFO')
module_logger = getLogger("database_logs")


class PostgresUtil:
    def __init__(self):
        self.connection = None

    async def connect(self, params_dic):
        """ Connect to the PostgreSQL database server """
        conn = None
        count_try = 0
        while count_try != 5:
            try:
                # connect to the PostgreSQL server
                module_logger.info(f">> Connecting to the PostgreSQL database...")
                conn = await asyncpg.connect(**params_dic)
                break
            except (Exception, asyncpg.DataError) as error:
                module_logger.error(error)
                await asyncio.sleep(1)
        if count_try == 5:
            sys.exit(1)
        module_logger.info(f">> Connection successful")
        return conn

    async def company_in_db(self, connection: Any = None, table_name: str = None, url: str = None):
        is_exist = await connection.fetchval(f'''select exists(select 1 from "{table_name}" as tn where '{url}' like '%' || tn.yandex_url || '%');''')
        if is_exist:
            module_logger.info(is_exist)
        return is_exist

    async def add_row_to_db(self,connection: Any = None, table_name: str = None, info_list: list = None):
        # self.connection = await self.connect({"database": "yandex", "host": "localhost", "user": "postgres", "password": "12345678"})
        company_id = None
        repeat = False

        for row in info_list:
            columns = tuple(row.keys())
            values = [row[col] for col in columns]
            if 'Review' not in table_name:
                is_exist = await connection.fetchval(f'''select exists(select 1 from "{table_name}" as tn where tn.yandex_url='{row['yandex_url']}');''')
                if not is_exist:
                    insert_statement = f'''insert into "{table_name}" (company_name, city, address, company_url, company_rating, logo, yandex_url) 
                    values {tuple(values)} returning id;
                    '''
                    company_id = await connection.fetchval(insert_statement)
                    module_logger.info(f"Company with name {row['company_name']} was added by id {company_id}.")
                else:
                    repeat = True
                    module_logger.warning(f"Such company already exist in database! {row['yandex_url']}")
                    company_id = await connection.fetchval(f'''select id from "{table_name}" tn where tn.yandex_url='{row['yandex_url']}'; ''')
                return company_id, repeat
            else:
                insert_statement = f'''
                insert into "{table_name}" (author_name, rating, city, date, time, text, company_id) values {tuple(values)};
                '''
                try:
                    await connection.fetchval(insert_statement)
                except asyncpg.UndefinedColumnError as col_err:
                    module_logger.warning(insert_statement)
                except (Exception, asyncpg.DataError) as error:
                    module_logger.error(str(error))

        # await self.connection.close()


    async def refactor_review(self, limit: int = 10):
        main_df = pd.DataFrame(columns=["id","text", "text_rewrite"])
        table_name = "Reviews"
        self.connection = await self.connect({"database": "yandex", "host": "localhost", "user": "postgres", "password": "12345678"})
        amount_of_revs = await self.connection.fetchval(f'select count(id) from "{table_name}";')
        module_logger.info(f"Start to rewrite reviews ({amount_of_revs}) with yandexGPT.")
        try:
            for _ in range(0, int(amount_of_revs) + 1, limit):
                query = f'''select id, text from "Reviews" as tn where tn.text_rewrite is null limit({str(limit)});'''
                values = await self.connection.fetch(query)
                rev_df = pd.DataFrame(values, columns=["id","text"])
                rev_df['text_rewrite'] = change_review(rev_df['text'].to_list())
                main_df = pd.concat([main_df, rev_df])
                module_logger.info(f"Append new rewrite reviews to dataframe, length of dataframe is {len(main_df)} ")
                module_logger.info(main_df)
                time.sleep(1)
            module_logger.info(f"reviews confirmed, total amount of reviews is {len(main_df)}")

            for ind, row in main_df.iterrows():
                query_update = f'''update "{table_name}" as tr set "text_rewrite"= '{row['text_rewrite']}' where tr.id={row['id']};'''
                await self.connection.fetch(query_update)
            module_logger.info(f"Rewrite review was done!")
            self.connection.close()
        except Exception as exc:
            module_logger.error(exc)
            self.connection.close()

pa = PostgresUtil()
asyncio.run(pa.refactor_review())
