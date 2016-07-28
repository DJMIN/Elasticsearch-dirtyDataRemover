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
import traceback
from threading import Thread
from elasticsearch import Elasticsearch, ElasticsearchException
import logging


es_hosts = '192.168.78.203:9200'
json_base_dir = 'setting/'

logger = logging.getLogger(name=__name__)    # 生成一个日志对象
logger.setLevel(logging.DEBUG)
log_file_handler = logging.FileHandler(os.path.join(os.getcwd(), 'log.txt'))   # 返回一个FileHandler对象
formatter = logging.Formatter('%(asctime)s:%(name)s-->%(levelname)s %(message)s')
log_file_handler.setFormatter(formatter)    # 将格式器设置到处理器上
logger.addHandler(log_file_handler)         # 将处理器加到日志对象上


def run_clean(json_dict):
    es_index = json_dict['index']
    search_title = json_dict['result.title']
    search_project = json_dict['project']
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
                    },
                    {
                        "match_phrase": {
                            "result.title": search_title
                        }
                    }
                ]
            }
        },
        "from": 0,
        "size": 50
    }

    try:
        es_client = Elasticsearch(hosts=es_hosts)
    except (ElasticsearchException, Exception) as ex:
        logger.warn('failed to connect to %s: %s' % (es_hosts, ex))

    while True:
        try:
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
        with codecs.open(json_files_base_dir + file_name, 'rb', 'utf-8') as fp:
            json_dict = json.loads(fp.read())
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
