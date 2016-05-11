# -*-coding:utf-8-*-
import time
import sys
import json
import numpy as np
from myconfig import * 
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
reload(sys)
sys.path.append('../../../')
from global_utils import es_user_portrait as es_9200
from global_utils import es_user_profile
from global_utils import flow_text_index_name_pre, flow_text_index_type
from global_utils import redis_flow_text_mid as r_flow
from parameter import DAY, RUN_TYPE
from time_utils import ts2datetime, datetime2ts, ts2date



def mapper_bci_today(todaydate):
    BCI_INDEX_NAME = "bci_" + ts2datetime((datetime2ts(todaydate) - DAY)).replace("-","")
    TODAY_TIME = todaydate
    print BCI_INDEX_NAME
    s_re = scan(es_9200, query={"query":{"match_all":{}},"size":MAX_ITEMS ,"fields":[TOTAL_NUM,TODAY_BCI, "user_fansnum", 'user_friendsnum']}, index=BCI_INDEX_NAME, doc_type=BCI_INDEX_TYPE)
    count = 0
    array = []
    while 1:
        try:
            temp = s_re.next()
            one_item = {}
            one_item['id'] = temp['_id'].encode("utf-8")
            one_item['user_fansnum'] = temp['fields']["user_fansnum"][0]
            one_item['user_friendsnum'] = temp['fields']['user_friendsnum'][0]
            one_item['total_num'] = temp['fields'][TOTAL_NUM][0]
            one_item['today_bci'] = temp['fields'][TODAY_BCI][0]
            one_item['update_time'] = TODAY_TIME
            array.append(one_item)
            count += 1
            if count % 1000 == 0:
                r_flow.lpush('update_bci_list', json.dumps(array))
                array = []
                #if count % 100000 == 0:
                #    print count
        except StopIteration: 
                print "all done" 
                if array:
                    r_flow.lpush('update_bci_list', json.dumps(array))              
                break 

    print count
    #s_re.close()

def mapper_bci_history(todaydate=None):
    if todaydate:
        TODAY_TIME = todaydate
    es_query = {"query":{"bool":{"must":[],"must_not":[{"term":{"bci.update_time":TODAY_TIME}}],"should":[{"match_all":{}}]}},"from":0,"size":MAX_ITEMS,"fields":[]}

    s_re = scan(es_user_profile, query=es_query, index=BCIHIS_INDEX_NAME, doc_type=BCIHIS_INDEX_TYPE)
    count = 0
    array = []
    while 1:
        try:
            temp = s_re.next()
            one_item = {}
            one_item['id'] = temp['_id'].encode("utf-8")
            one_item['total_num'] = 0
            one_item['today_bci'] = 0
            one_item['update_time'] = TODAY_TIME
            array.append(one_item)
            count += 1
            if count % 1000 == 0:
                r_flow.lpush('update_bci_list', json.dumps(array))
                array = []
                if count % 100000 == 0:
                    print count
        except StopIteration: 
                print "all done" 
                if array:
                    r_flow.lpush('update_bci_list', json.dumps(array))
                break 

    print count


if __name__ == '__main__':
    todaydate = ts2datetime(time.time())
    #todaydate = '2016-04-19'
    print todaydate
    print "push_bci_redis&start&%s" %ts2date(time.time())
    ts = datetime2ts(todaydate)
    #print es_user_profile.indices.put_mapping(index="bci_history", doc_type="bci", body={'properties':{"user_friendsnum":{"type":"long"}}})
    try:
        mapper_bci_today(todaydate)
    except Exception, e:
        print e, '&error&', ts2date(time.time())
    print "push_bci_redis&end%s" %ts2date(time.time())
