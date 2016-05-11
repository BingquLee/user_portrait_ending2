# -*- coding:utf-8 -*-
import os
import sys

reload(sys)
sys.path.append('../../')
from user_portrait.global_utils import es_user_portrait, portrait_index_name,\
                      portrait_index_type, es_sentiment_task, sentiment_keywords_index_name ,\
                      sentiment_keywords_index_type, R_SENTIMENT_KEYWORDS,\
                      r_sentiment_keywords_name
from time_utils import ts2datetime, datetime2ts

#use to read task information from redis queue
def get_task_information():
    try:
        results = R_SENTIMENT_KEYWORDS.rpop(r_sentiment_keywords_name)
    except:
        results = {}
    if result:
        task_information_dict = json.loads(results)
    else:
        task_information_dict = {}

    return task_information_dict

#use to identify the task is exist in es
def identify_task_exist(task_information_dict):
    task_id = task_information_dict['task_id']
    try:
        task_exist = es_sentiment_task.get(index=sentiment_keywords_index_name, \
                doc_type=sentiment_keywords_index_type, id=task_id)['_source']
    except:
        task_exist = {}
    if not task_exist:
        return False
    else:
        return True


#use to read task information from queue
def scan_sentiment_keywords_task():
    #step1: read task information from reids queue
    #step2: identify the task information is exist in es
    #step3: compute the sentiment trend task
    while True:
        #read task informaiton from redis queue
        sentiment_task_information = get_task_information()
        #when redis queue null - file break
        if not sentiment_task_information:
            break
        #identify the task is exist in es
        exist_mark = identify_task_exist(sentiment_task_information)
        if exist_mark:
            results = compute_sentiment_task(sentiment_task_information)
            #save results
            save_mark = save_task_results(results)
            #identify save status
            if not save_mark:
                #status fail: push task information to redis queue
                push_task_information(sentiment_task_information)
        else:
            #if no exist - pass
            pass



if __name__=='__main__':
    log_time_ts = time.time()
    print 'cron/sentiment/cron_sentiment.py&start&' + str(log_time_ts)

    scan_sentiment_keywords_task()

    log_time_ts = time.time()
    print 'cron/senitment/cron_sentiment.py&end&' + str(log_time_ts)
