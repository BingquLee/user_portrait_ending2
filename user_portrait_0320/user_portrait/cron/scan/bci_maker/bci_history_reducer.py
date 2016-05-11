# -*-coding:utf-8-*-
# es user profile ======10.128.55.81
import time
import sys
import redis
import json
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from bci_history_calculator import cal_num_for_bci_history
from myconfig import * 
from bci_history_mapping import bci_history_mapping
reload(sys)
sys.path.append('../../../')
from global_utils import es_user_profile as es
from global_utils import flow_text_index_name_pre, flow_text_index_type
from global_utils import redis_flow_text_mid as r_flow
from parameter import DAY, RUN_TYPE
from time_utils import ts2datetime, datetime2ts, ts2date

def reducer():
    count = 0
    ts = time.time()
    while 1:
        user_set = r_flow.rpop('update_bci_list')
        bulk_action = []
        if user_set:
            items = json.loads(user_set)
            uid_list = []
            for item in items:
                uid_list.append(item['id'])
            if uid_list:
                search_results = es.mget(index=BCIHIS_INDEX_NAME, doc_type=BCIHIS_INDEX_TYPE, body={"ids":uid_list})["docs"]
                cal_num_for_bci_history(uid_list, items, search_results)
                count += len(uid_list)
                if count % 10000 == 0:
                    te = time.time()
                    #print "count: %s, cost time: %s" %(count, te-ts)
                    ts = te
        else:
            print count
            break


if __name__ == "__main__":
    time.sleep(100)
    ts = time.time()
    print "all_bci&start&%s"%ts2date(ts)
    try:
        reducer()
    except Exception, e:
        print e, '&error&', ts2date(time.time())
    print "all_bci&end&%s"%ts2date(time.time())
 
