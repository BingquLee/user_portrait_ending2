# -*- coding: UTF-8 -*-
'''
acquire the profile information from user_profile
input: uid_list
output: {uid:{attr:value}}
'''
import sys
import json
reload(sys)
sys.path.append('../../')
from global_utils import es_user_profile as es
from global_utils import profile_index_name as index_name
from global_utils import profile_index_type as index_type
from global_utils import es_bci_history, bci_history_index_name, bci_history_index_type
fields_dict = {'uname':"nick_name", 'gender':"sex", 'location':"user_location", \
               'verified':"isreal", 'fansnum':"fansnum", 'statusnum':"statusnum", \
               'friendsnum':"friendsnum", 'photo_url':"photo_url"}


def get_profile_information(uid_list):
    result_dict = dict()
    search_result = es.mget(index=index_name, doc_type=index_type, body={'ids':uid_list}, _source=True)['docs']
    try:
        bci_history_result = es_bci_history.mget(index=bci_history_index_name, doc_type=bci_history_index_type, body={'ids': uid_list}, fields=['user_fansnum', 'weibo_month_sum', 'user_friendsnum'])['docs']
    except:
        bci_history_result = []
    iter_count = 0
    for item in search_result:
        user_dict = {}
        try:
            bci_history_item = bci_history_result[iter_count]
        except:
            bci_history_item = {}
        for field in fields_dict:
            try:
                if field == 'statusnum':
                    if bci_history_item and bci_history_item['found']==True:
                        if isinstance(bci_history_item['fields']['weibo_month_num'][0], int):
                            user_dict[field] = bci_history_item['fields']['weibo_month_sum'][0]
                        else:
                            user_dict[field] = 0
                    else:
                        user_dict[field] = 0
                elif field == 'fansnum':
                    if bci_history_item and bci_history_item['found']==True:
                        if isinstance(bci_history_item['fields']['user_fansnum'][0], int):
                            user_dict[field] = bci_history_item['fields']['user_fansnum'][0]
                        else:
                            user_dict[field] = 0
                    else:
                        user_dict[field] = 0
                elif field == 'friendsnum':
                    if bci_history_item and bci_history_item['found']==True:
                        if isinstance(bci_history_item['fields']['user_friendsnum'][0], int):
                            user_dict[field] = bci_history_item['fields']['user_friendsnum'][0]
                        else:
                            user_dict[field] = 0
                    else:
                        user_dict[field] = 0
                else:
                    try:
                        user_dict[field] = item['_source'][field]
                    except:
                        user_dict[field] = ''
            except:
                if field=='statusnum':
                    user_dict[field] = 0
                elif field=='fansnum':
                    user_dict[field] =0
                elif field=='friendsnum':
                    user_dict[field] = 0
                elif field=='gender':
                    user_dict[field] = 0
                elif field=='uname':
                    user_dict[field] = u'unknown'
                else:
                    user_dict[field] = 'unknown'
        result_dict[item['_id']] = user_dict
        iter_count += 1
    return result_dict

if __name__=="__main__":
    test_uid = ['1223354542', '2234766704']
    result = get_profile_information(test_uid)
    print 'result:', result

