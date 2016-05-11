# -*-coding:utf-8-*-
import time
import sys
import redis
import numpy as np
from elasticsearch import Elasticsearch
from myconfig import *
reload(sys)
sys.path.append('../../../')
from global_utils import es_user_profile as es
from global_utils import flow_text_index_name_pre, flow_text_index_type
from parameter import DAY, RUN_TYPE, MONTH_TIME

BCIHIS_INDEX_NAME = 'bci_history'
BCIHIS_INDEX_TYPE = 'bci'


def var_cal(n , old_var , old_ave , new_ave , new_bci):
    return ( n - 1) / n * (old_var + np.power(old_ave - new_ave , 2) ) + np.power(new_bci - new_ave , 2) / n

def cal_num_for_bci_history(uid_list , items , results):
    count = 0
    bulk_action = []
    ts = time.time()
    #now_ts = datetime2ts('2016-04-19')
    now_ts = time.time()
    timestamp = datetime2ts(ts2datetime(now_ts-DAY))
    del_ts = datetime2ts(ts2datetime(now_ts-MONTH_TIME))
    update_key = "bci_" + str(timestamp)
    delete_key = "bci_" + str(del_ts)
    weibo_update_key = "weibo_" + str(timestamp)
    weibo_delete_key = "weibo_" + str(del_ts)
    for i in range(len(uid_list)):
        today_bci = items[i]['today_bci']
        total_num = items[i]['total_num']
        update_time = items[i]['update_time']
        fansnum = items[i]["user_fansnum"]
        friendsnum = items[i]['user_friendsnum']
        uid = uid_list[i]
        item = dict()
        if results[i]['found']:
            iter_result = results[i]['_source']
            during = iter_result['during']
            uid = uid_list[i]
            bci_day_last = iter_result['bci_day_last']
            bci_day_change = iter_result['bci_day_change']
            bci_day_var = iter_result['bci_day_var'] 
            bci_day_ave = iter_result['bci_day_ave'] 
            bci_week_num = iter_result['bci_week_sum']
            bci_week_change = iter_result['bci_week_change']
            bci_week_var = iter_result['bci_week_var'] 
            bci_week_ave = iter_result['bci_week_ave'] 
            bci_month_num = iter_result['bci_month_sum']
            bci_month_change = iter_result['bci_month_change']
            bci_month_var = iter_result['bci_month_var'] 
            bci_month_ave = iter_result['bci_month_ave']

            weibo_day_last = iter_result['weibo_day_last']
            weibo_week_num = iter_result['weibo_week_sum']
            weibo_month_num = iter_result['weibo_month_sum']
            
            n = during
            bci_delete = 0
            weibo_delete = 0
            iter_result[update_key] = today_bci
            if delete_key in iter_result:
                bci_delete = iter_result.pop(delete_key)
            iter_result[weibo_update_key] = total_num
            if weibo_delete_key in iter_result:
                weibo_delete = iter_result.pop(weibo_delete_key)
            iter_result['bci_day_last'] = today_bci 
            iter_result['bci_day_change'] = today_bci - bci_day_last
            iter_result['bci_day_ave'] = (bci_day_ave * n + today_bci) / ( n + 1)
            iter_result['bci_day_var'] = var_cal(n+1 , bci_day_var , bci_day_ave , iter_result['bci_day_ave'] ,today_bci )
            if n < 7:
                week_dividend = n 
            else:
                week_dividend = 7
            
            iter_result['bci_week_sum'] = bci_week_num - bci_delete + today_bci
            iter_result['bci_week_change'] =  today_bci - iter_result['bci_week_ave']
            iter_result['bci_week_ave'] = iter_result['bci_week_sum'] / week_dividend
            iter_result['bci_week_var'] = var_cal( week_dividend , bci_week_var , bci_week_ave , iter_result['bci_week_ave'] ,today_bci )
            
            if n < 30:
                month_dividend = n 
            else:
                month_dividend = 30
            iter_result['bci_month_sum'] = bci_month_num - bci_delete + today_bci
            iter_result['bci_month_change'] = today_bci - iter_result['bci_month_sum']
            iter_result['bci_month_ave'] = iter_result['bci_month_sum'] / month_dividend
            iter_result['bci_month_var'] = var_cal( month_dividend , bci_month_var , bci_month_ave , iter_result['bci_month_ave'] ,today_bci )
            
            iter_result['weibo_day_last'] = total_num
            if n <= 7:
                iter_result['weibo_week_sum'] = weibo_week_num + total_num
            else :
                iter_result['weibo_week_sum'] = weibo_week_num + total_num - weibo_delete

            if n <= 30:
                iter_result['weibo_month_sum'] = weibo_month_num + total_num
            else:
                iter_result['weibo_month_sum'] = weibo_month_num + total_num - weibo_delete

            iter_result['during'] = n + 1
            iter_result['update_time'] = update_time
            iter_result['user_fansnum'] = fansnum
            iter_result['user_friendsnum'] = friendsnum
            item = iter_result
            item['uid'] = uid
            
        else:
            item[update_key] = today_bci
            item[weibo_update_key] = total_num
            item['user_fansnum'] = fansnum
            item['user_friendsnum'] = friendsnum
            item['bci_day_last'] = today_bci 
            item['bci_day_change'] = today_bci
            item['bci_day_ave'] = today_bci
            item['bci_day_var'] = 0
            item['bci_week_sum'] = today_bci
            item['bci_week_change'] = today_bci
            item['bci_week_ave'] = today_bci
            item['bci_week_var'] = 0            
            item['bci_month_sum'] = today_bci
            item['bci_month_change'] = today_bci
            item['bci_month_ave'] = today_bci
            item['bci_month_var'] = 0           
            item['weibo_day_last'] = total_num     
            item['weibo_week_sum'] = total_num 
            item['weibo_month_sum'] = total_num 
            item['during'] = 1
            item['update_time'] = update_time
            item['uid'] = uid
            

        action = {'index':{'_id': uid}}
        bulk_action.extend([action, item])
        count += 1
        
    if bulk_action:
        es.bulk(bulk_action, index = BCIHIS_INDEX_NAME , doc_type = BCIHIS_INDEX_TYPE, timeout=600)
    te = time.time()
    #print "count: %s, cost time: %s" %(count, te-ts)

