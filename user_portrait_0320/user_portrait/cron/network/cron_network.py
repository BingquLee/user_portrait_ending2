# -*- coding:utf-8 -*-
import tempfile
import sys
import json
import time
import tempfile
reload(sys)
sys.path.append('../../')
from spam.pagerank_for_portrait import pagerank
from time_utils import ts2datetime, datetime2ts, ts2date
from keywords_utils import get_task_information, identify_task_exist,\
        compute_network_task, write_tmp_file, save_task_results,\
        push_task_information

#use to read task information from queue
def scan_network_keywords_task():
    #step1: read task information from redis queue
    #step2: identify the task information is exist in es
    #step3: compute the network trend task
    while True:
        #read task informaiton from redis queue
        network_task_information = get_task_information()
        print network_task_information
        #when redis queue null - file break
        if not network_task_information:
            break
        #identify the task is exist in es
        exist_mark = identify_task_exist(network_task_information)
        print 'exist_mark:', exist_mark
        if exist_mark:
            print 'step 1: compute', ts2date(time.time())
            results = compute_network_task(network_task_information)
            if results:
                tmp_file = tempfile.NamedTemporaryFile(delete=False)
                write_tmp_file(tmp_file, results)
                tmp_file.close()

                if not tmp_file:
                    return
                input_tmp_path = tmp_file.name
                print input_tmp_path

                ITER_COUNT = 10
                TOP_N = 50
                print 'step 2: pagerank', ts2date(time.time())
                all_uids_count, dg_sorted_uids, pr_sorted_uids = pagerank(ITER_COUNT, input_tmp_path, TOP_N, 'keywords')
                #save results
                print 'step 3: save', ts2date(time.time())
                save_mark = save_task_results(dg_sorted_uids, pr_sorted_uids, network_task_information)
                print 'save done', ts2date(time.time())
                #identify save status
                if not save_mark:
                    #status fail: push task information to redis queue
                    push_mark = push_task_information(network_task_information)
                    if not push_mark:
                        print 'error push task queue'
        else:
            #if no exist - pass
            pass


if __name__=='__main__':
    log_time_ts = time.time()
    log_time_date = ts2date(log_time_ts)
    print 'cron/network/cron_network.py&start&' + log_time_date
    
    try: 
        scan_network_keywords_task()
    except Exception, e:
        print e, '&error&', ts2date(time.time())

    log_time_ts = time.time()
    log_time_date = ts2date(log_time_ts)
    print 'cron/network/cron_network.py&end&' + log_time_date
