# -*- coding: utf-8 -*-

import time
import tempfile
import sys
sys.path.append('../../')
from cron_user_portrait_network_mappings import network_es_mappings, network_count_es_mappings
from spam.pagerank_for_portrait import pagerank
from utils import scan_retweet, save_dg_pr_results, get_es_num, save_count_results
from time_utils import ts2datetime, datetime2ts, ts2date

def pagerank_rank():
    timestamp = time.time()
    es_num = get_es_num(timestamp)
    if es_num == 0:
        network_es_mappings()
        network_count_es_mappings()

    tmp_file = tempfile.NamedTemporaryFile(delete=False)

    print 'step 1: scan', ts2date(timestamp)
    scan_retweet(tmp_file)
    tmp_file.close()
    if not tmp_file:
        return
    input_tmp_path = tmp_file.name
    print input_tmp_path

    ITER_COUNT = 10
    TOP_N = 50
    print 'step 2: pagerank', ts2date(time.time())
    all_uids_count, dg_sorted_uids, pr_sorted_uids = pagerank(ITER_COUNT, input_tmp_path, TOP_N, 'all')
    print 'step 3: save', ts2date(time.time())
    save_count_results(all_uids_count, es_num)
    save_dg_pr_results(dg_sorted_uids, es_num, 'dg')    
    save_dg_pr_results(pr_sorted_uids, es_num, 'pr')    
    print 'save done', ts2date(time.time())


if __name__ == '__main__':
    try:
        pagerank_rank()
    except Exception,e:
        print e, '&error&', ts2date(time.time())

