#!/usr/bin/env python
# -*- coding:utf-8 -*- 
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:

"""
@version: 1.0
@author: readerror
@contact: readerror000@gmail.com
@site: http://www.readerror.cn
@software: PyCharm
@file: run.py
@time: 2016/7/25 11:09
"""

import os
import codecs
import json
import sys
from threading import Thread
from elasticsearch import Elasticsearch, ElasticsearchException
import logging

global es_hosts
es_hosts = "127.0.0.1:9200"

"""
# windows version
with codecs.open('es_data_remover/config.json', 'rb', 'utf-8') as _:
    es_hosts = json.loads(_.read())["es_hosts"]
json_base_dir = 'es_data_remover/setting/'
log_file_handler = logging.FileHandler(os.path.join(os.getcwd(), 'es_data_remover.log'))
"""

# """
# linux version
with codecs.open('/etc/es_data_remover/config.json', 'rb', 'utf-8') as _:
    es_hosts = json.loads(_.read())["es_hosts"]
json_base_dir = '/etc/es_data_remover/setting/'
log_file_handler = logging.FileHandler(os.path.join(os.getcwd(), '/var/log/es_data_remover.log'))
# """

logger = logging.getLogger(name=__name__)    # 生成一个日志对象
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(name)s-->%(levelname)s %(message)s')
log_file_handler.setFormatter(formatter)    # 将格式器设置到处理器上
logger.addHandler(log_file_handler)         # 将处理器加到日志对象上


def run_clean(json_dict):
    global es_client
    try:
        es_client = Elasticsearch(hosts=es_hosts)
    except (ElasticsearchException, Exception) as ex:
        logger.warn('failed to connect to %s: %s' % (es_hosts, ex))

    es_index = json_dict.pop('index')
    search_project = json_dict.pop('project')
    search_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "project": [
                                search_project
                            ]
                        }
                    }
                ]

            }
        },
        "from": 0,
        "size": 50
    }

    for key, value in json_dict.items():
        search_body["query"]["bool"]["must"].append({
            "match_phrase": {
                key: value
             }
        })

    while True:
        try:
            logger.info('searching %s:%s from %d to %d ' %
                        (es_index, search_body["query"]["bool"]["must"],
                         search_body["from"], search_body["from"] + search_body["size"]))
            es_response = es_client.search(
                index=es_index,
                body=search_body,
                _source=False
            )
        except (ElasticsearchException, Exception) as ex:
            logger.warn('failed to search %s: %s' % (search_body, ex))
            break

        result_len = len(es_response['hits']['hits'])
        logger.info("search %d results" % result_len)
        if result_len == 0:
            break
        search_body["from"] += result_len

        for hit in es_response['hits']['hits']:
            logger.info("deleting %s" % (hit["_id"]))
            # self.es_client.delete(index=self.es_index, doc_type=self.__type__, id=hit["_id"])


def get_files_list(base_dir):
    files_list = os.listdir(base_dir)
    return files_list


def reader_process(file_name, json_files_base_dir):
    try:
        with codecs.open(json_files_base_dir + file_name, 'rb', 'utf-8') as _:
            json_dict = json.loads(_.read())
            run_clean(json_dict)
    except Exception:
        info = sys.exc_info()
        logger.warn("reader_process: %s %s", info[0], ":", info[1])


if __name__ == '__main__':
    json_file_list = get_files_list(json_base_dir)
    processes = []

    for j in json_file_list:
        reader = Thread(target=reader_process, args=(j, json_base_dir,))
        reader.start()
        processes.append(reader)

    for p in processes:
        p.join()
