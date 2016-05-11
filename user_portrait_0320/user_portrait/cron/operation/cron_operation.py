# -*- coding: UTF-8 -*-
'''
use to statistic all user operation infor
'''
import sys
import time
from sqlite_query import get_user_name

reload(sys)
sys.path.append('../../')
from global_utils import R_RECOMMENTATION, es_tag, tag_index_name, tag_index_type, \
                         es_group_result, group_index_name, group_index_type, \
                         es_social_sensing, sensing_index_name, sensing_doc_type, \
                         es_sentiment_task, sentiment_keywords_index_name, \
                         sentiment_keywords_index_type, es_network_task, \
                         network_keywords_index_name, network_keywords_index_type, \
                         es_rank_task, rank_keywords_index_name, rank_keywords_index_type, \
                         es_operation, operation_index_name, operation_index_type
from time_utils import ts2datetime, datetime2ts
from parameter import DAY

def get_recommentation(admin_user):
    submit_recommentation_count = 0
    compute_count = 0
    search_date = ts2datetime(time.time() - DAY)
    submit_recomment_key = 'recomment_' + admin_user + '_' + search_date
    submit_user_recomment = set(R_RECOMMENTATION.hkeys(submit_recomment_key))
    all_compute_set = set(R_RECOMMENTATION.hkeys('compute'))
    submit_recommentation_count = len(submit_user_recomment)
    compute_count = len(submit_user_recomment & all_compute_set)
    
    return submit_recommentation_count, compute_count

def get_rank(admin_user):
    rank_count = 0
    #query_body
    search_date = ts2datetime(time.time() - DAY)
    query_body = {
    'query': {
        'bool': {
            'must':[
                {'term': {'submit_user': admin_user}},
                {'term': {'submit_time': search_date}}
                ]
            }
        }
    }
    rank_count = es_rank_task.count(index=rank_keywords_index_name, doc_type=rank_keywords_index_type, body=query_body)['count']

    return rank_count

def get_sentiment(admin_user):
    sentiment_task_count = 0
    #query_body
    search_timestamp_end = datetime2ts(ts2datetime(time.time()))
    search_timestamp_start = search_timestamp_end - DAY
    query_body = {
    'query':{
        'bool':{
            'must':[
                {'term': {'submit_user': admin_user}},
                {'range': {'submit_ts': {'gte': search_timestamp_start, 'lt': search_timestamp_end}}}
                ]
            }
        }
    }
    sentiment_task_count = es_sentiment_task.count(index=sentiment_keywords_index_name, doc_type=sentiment_keywords_index_type, body=query_body)['count']
    return sentiment_task_count

def get_network(admin_user):
    network_task_count = 0
    #query_body
    end_ts = datetime2ts(ts2datetime(time.time()))
    start_ts = end_ts - DAY
    query_body = {
    'query':{
        'bool':{
            'must':[
                {'term': {'submit_user': admin_user}},
                {'range': {'submit_ts': {'gte': start_ts, 'lt': end_ts}}}
                ]
            }
        }
    }
    network_task_count = es_network_task.count(index=network_keywords_index_name, doc_type=network_keywords_index_type, body=query_body)['count']
    return network_task_count

def get_group_detect(admin_user):
    group_detect_count = 0
    #query body
    end_ts = datetime2ts(ts2datetime(time.time()))
    start_ts = end_ts - DAY
    query_body = {
    'query':{
        'bool':{
            'must':[
                {'term': {'submit_user': admin_user}},
                {'term': {'task_type': 'detect'}},
                {'range': {'submit_date': {'gte': start_ts, 'lt': end_ts}}}
                ]
            }
        }
    }
    group_detect_count = es_group_result.count(index=group_index_name, doc_type=group_index_type, body=query_body)['count']

    return group_detect_count

def get_group_analysis(admin_user):
    group_analysis_count = 0
    #query body
    end_ts = datetime2ts(ts2datetime(time.time()))
    start_ts = end_ts - DAY
    query_body = {
    'query':{
        'bool':{
            'must': [
                {'term': {'submit_user': admin_user}},
                {'term': {'task_type': 'analysis'}},
                {'range':{'submit_date': {'gte': start_ts, 'lt': end_ts}}}
                ]
            }
        }
    }
    group_analysis_count = es_group_result.count(index=group_index_name, doc_type=group_index_type, body=query_body)['count']

    return group_analysis_count

def get_sensing(admin_user):
    sensing_task_count = 0
    #query_body
    end_ts = datetime2ts(ts2datetime(time.time()))
    start_ts = end_ts - DAY
    query_body = {
    'query':{
        'bool':{
            'must':[
                {'term': {'create_by': admin_user}},
                {'range': {'create_at': {'gte': start_ts, 'lt': end_ts}}}
                ]
            }
        }
    }
    sensing_task_count = es_social_sensing.count(index=sensing_index_name, doc_type=sensing_doc_type, body=query_body)['count']

    return sensing_task_count

def get_tag(admin_user):
    tag_count = 0
    submit_date = ts2datetime(time.time() - DAY)
    query_body = {
    'query':{
        'bool':{
            'must':[
                {'term': {'date': submit_date}},
                {'term': {'user': admin_user}}
                ]
            }
        }
    }
    tag_count = es_tag.count(index=tag_index_name, doc_type=tag_index_type, body=query_body)['count']
    return tag_count

def update_recommentation_compute(admin_user):
    status = False
    #step1: update lastest 6 day compute count
    end_ts = datetime2ts(ts2datetime(time.time() - DAY))
    all_compute_set = set(R_RECOMMENTATION.hkeys('compute'))
    bulk_action = []
    for i in range(1, 6):
        iter_ts = end_ts - i* DAY
        iter_date = ts2datetime(iter_ts)
        submit_recomment_key = 'recomment_' + admin_user + '_' + iter_date
        submit_recomment_set = set(R_RECOMMENTATION.hkeys(submit_recomment_key))
        compute_count = len(submit_recomment_set & all_compute_set)
        user_results = {'compute_count': compute_count}
        action = {'update': {'_id': admin_user + '_' + str(iter_ts)}}
        bulk_action.extend([action, {'doc': user_results}])
    #step2: update bulk action
    #print 'bulk_action:', bulk_action
    es_operation.bulk(bulk_action, index=operation_index_name, doc_type=operation_index_type)
    status = True
    return status

def statistic_all_operation():
    #step1: get user name list from db
    admin_user_list = get_user_name()
    #step2: iter to get admin user operation info
    bulk_action = []
    for admin_user in admin_user_list:
        results = {}
        add_date_ts = datetime2ts(ts2datetime(time.time() - DAY))
        results['timestamp'] = add_date_ts
        results['admin_user']  = admin_user
        #step2.1: get user submit recommentation in count
        submit_recommentation_count, compute_count = get_recommentation(admin_user)
        results['recomment_count'] = submit_recommentation_count
        results['compute_count'] = submit_recommentation_count
        #step2.1.2: update user submit recommentation compute
        update_recommentation_compute(admin_user)
        #step2.2: get user submit rank task
        submit_rank_task_count = get_rank(admin_user)
        results['rank_count'] = submit_rank_task_count
        #step2.3: get user submit sentiment task
        submit_sentiment_count = get_sentiment(admin_user)
        results['sentiment_count'] = submit_sentiment_count
        #step2.4: get user submit network task
        submit_network_count = get_network(admin_user)
        results['network_count'] = submit_network_count
        #step2.5: get user submit group detect task
        submit_group_detect_count = get_group_detect(admin_user)
        results['detect_count'] = submit_group_detect_count
        #step2.6: get user submit group analysis task
        submit_group_analysis_count = get_group_analysis(admin_user)
        results['analysis_count'] = submit_group_analysis_count
        #step2.7: get user submit social sensing task
        submit_sensing_count = get_sensing(admin_user)
        results['sensing_count'] = submit_sensing_count
        #step2.8: get user add tag
        submit_tag_count = get_tag(admin_user)
        results['tag_count'] = submit_tag_count
        #step2.9: save bulk action
        add_date_ts = datetime2ts(ts2datetime((time.time() - DAY)))
        action = {'index': {'_id': admin_user + '_' + str(add_date_ts)}}
        bulk_action.extend([action, results])
    #save bulk action
    #print 'bulk_action:', bulk_action
    es_operation.bulk(bulk_action, index=operation_index_name, doc_type=operation_index_type, timeout=60)

if __name__=='__main__':
    log_time_start_ts = time.time()
    log_time_start_date = ts2datetime(log_time_start_ts)
    print 'cron/operation/cron_operation.py&start&' + log_time_start_date
    
    try:
        statistic_all_operation()
    except Exception, e:
        print e , '&error&' , ts2date(time.time())

    log_time_end_ts = time.time()
    log_time_end_date = ts2datetime(log_time_end_ts)
    print 'cron/operation/cron_operation.py&end&' + log_time_end_date
