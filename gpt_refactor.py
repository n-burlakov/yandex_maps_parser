import requests
import json
import time
import os
import logging
import coloredlogs
from utils.wutil import getLogger

coloredlogs.install(level='INFO')
module_logger = getLogger("refactor_reviews_log")

def get_json_data(reviews, directory_id):
    list_of_messages = []
    list_of_messages.append({
        "role": "user",
        "text": f"""Перепиши своими словами: {reviews}"""
    })
    json_req = {
        "modelUri": f"gpt://{directory_id}/yandexgpt-lite",
        "completionOptions": {
            "stream": False,
            "temperature": 0.8,
            "maxTokens": "2000"
        },
        "messages": list_of_messages
    }
    return json_req


def get_creds():
    directory_id = "b1g0a2e2wergewrgqwe2"  # это идентификатор каталога в апи яндекса
    with open(os.path.expanduser('/root/iam_token.txt'), 'r') as f:  # каждые 12 часов запускается крон на обмен <iam token> выглядит так: yc iam create-token > iam_token.txt
        iam_token = f.read().split('\n')[0]
    return iam_token, directory_id


def change_review(review_text_list: list = None):
    iam_token, directory_id = get_creds()
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {iam_token}", "x-folder-id": directory_id}
    url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    result = list()
    for review in review_text_list:
        json_req = get_json_data(review, directory_id)
        resp = requests.post(url, data=json.dumps(json_req), headers=headers)
        responce = resp.json()
        if 'result' in responce:
            result.append(responce['result']['alternatives'][0]['message']['text'])
        else:
            module_logger.error(str(responce))
            raise responce['error']['message']
    module_logger.info(f"refactoring review successfully. result is: {result}")
    return result
