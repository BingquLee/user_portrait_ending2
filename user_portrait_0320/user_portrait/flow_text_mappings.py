# -*- coding: UTF-8 -*-
'''
use to save text from flow_text --for all people 7day
'''
from elasticsearch import Elasticsearch
from global_utils import es_flow_text as es


def get_mappings(index_name):
    index_info = {
            'settings':{
                'analysis':{
                    'analyzer':{
                        'my_analyzer':{
                            'type': 'pattern',
                            'pattern': '&'
                        }
                    }
                }
            },
            'mappings':{
                'text':{
                    'properties':{
                        'text':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'mid':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'ip':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'directed_uid':{
                            'type':'long',
                            },
                        'directed_uname':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'timestamp':{
                            'type': 'long'
                            },
                        'sentiment': {
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'geo':{
                            'type': 'string',
                            'analyzer': 'my_analyzer'
                            },
                        'keywords_dict':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'keywords_string':{
                            'type': 'string',
                            'analyzer': 'my_analyzer'
                            },
                        'sensitive_words_dict':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'sensitive_words_string':{
                            'type': 'string',
                            'analyzer': 'my_analyzer'
                            },
                        'message_type':{
                            'type': 'long'
                            },
                        'uid':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'root_uid':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'root_mid':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                         # uncut weibo text
                        'origin_text':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'origin_keywords_dict':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'origin_keywords_string':{
                            'type': 'string',
                            'analyzer': 'my_analyzer'
                            }
                        }
                    }
                }
            }
    exist_indice = es.indices.exists(index=index_name)
    if not exist_indice:
        es.indices.create(index=index_name, body=index_info, ignore=400)

if __name__=='__main__':
    #index_name = 'flow_text_2016-03-21'
    get_mappings(index_name)
    #es.indices.put_mapping(index="flow_text_2016-04-12", doc_type="text", body={'properties':{"user_fansnum":{"type":"long"}}}, ignore=400)
