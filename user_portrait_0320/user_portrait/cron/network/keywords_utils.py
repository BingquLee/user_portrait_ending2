# -*- coding:utf-8 -*-
import os
import sys
import json
import time
reload(sys)
sys.path.append('../../')
from global_utils import R_NETWORK_KEYWORDS, r_network_keywords_name
from global_utils import es_network_task, network_keywords_index_name, \
        network_keywords_index_type, es_flow_text, flow_text_index_name_pre,\
        flow_text_index_type
from time_utils import ts2datetime, datetime2ts, ts2date
from parameter import DAY

#use to read task information from redis queue
def get_task_information():
    try:
        results = R_NETWORK_KEYWORDS.rpop(r_network_keywords_name)
    except:
        results = {}
    if results:
        task_information_dict = json.loads(results)
    else:
        task_information_dict = {}

    return task_information_dict

#use to identify the task is exist in es
def identify_task_exist(task_information_dict):
    task_id = task_information_dict['task_id']
    try:
        task_exist = es_network_task.get(index=network_keywords_index_name, \
                doc_type=network_keywords_index_type, id=task_id)['_source']
    except:
        task_exist = {}
    if not task_exist:
        return False
    else:
        return True

def save_task_results(dg_sorted_uids, pr_sorted_uids, network_task_information):
    status = True
    results = {}
    results['dg'] = dg_sorted_uids
    results['pr'] = pr_sorted_uids
    #step1:identify the task is exist in es
    exist_mark = identify_task_exist(network_task_information)
    #step2:add the task results to es
    if exist_mark:
        task_id = network_task_information['task_id']
        #try:
        es_network_task.update(index=network_keywords_index_name, \
                doc_type=network_keywords_index_type, id=task_id, body={'doc': {'results':json.dumps(results), 'status': '1'}})
        #except:
        #    status = False
    return status

def push_task_information(network_task_information):
    status = True
    try:
        R_NETWORK_KEYWORDS.lpush(r_network_keywords_name, json.dumps(network_task_information))
    except:
        status = False
    return status

#use to compute task network
def compute_network_task(network_task_information):
    results = {}
    #step1: get task information
    start_date = network_task_information['start_date']
    start_ts = datetime2ts(start_date)
    end_date = network_task_information['end_date']
    end_ts = datetime2ts(end_date)
    iter_date_ts = start_ts
    to_date_ts = end_ts
    iter_query_date_list = [] # ['2013-09-01', '2013-09-02']
    while iter_date_ts <= to_date_ts:
        iter_date = ts2datetime(iter_date_ts)
        iter_query_date_list.append(iter_date)
        iter_date_ts += DAY
    #step2: get iter search flow_text_index_name
    #step2.1: get search keywords list
    query_must_list = []
    keyword_nest_body_list = []
    keywords_string = network_task_information['query_keywords']
    keywords_list = keywords_string.split('&')
    for keywords_item in keywords_list:
        keyword_nest_body_list.append({'wildcard': {'text': '*' + keywords_item + '*'}})
    query_must_list.append({'bool': {'should': keyword_nest_body_list}})
    query_must_list.append({'term': {'message_type': '3'}})
    #step2.2: iter search by date
    results = []
    for iter_date in iter_query_date_list:
        flow_text_index_name = flow_text_index_name_pre + iter_date
        query_body = {
            'query':{
                'bool':{
                    'must':query_must_list
                }
            }
        }
        flow_text_result = es_flow_text.search(index=flow_text_index_name, doc_type=flow_text_index_type,\
                    body=query_body)['hits']['hits']
        results.extend(flow_text_result)
    return results

def write_tmp_file(tmp_file, results):
    count = 0
    for item in results:
        source = item['_source']
        uid = source['uid']
        directed_uid = source['directed_uid']
        if (uid != directed_uid):
            count += 1
            tmp_file.write('%s\t%s\n' % (uid, directed_uid))
    tmp_file.flush()

