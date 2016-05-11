import time
import sys
import redis
from elasticsearch import Elasticsearch
reload(sys)
sys.path.append('../../../')
from global_utils import es_user_profile as es

def bci_history_mapping():
    index_info = {
        "mappings":{
            "bci":{
                "properties":{
                    "uid":{
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "during":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "bci_day_last":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "bci_day_change":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "bci_day_var":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "bci_day_ave":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "bci_week_sum":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "bci_week_change":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "bci_week_ave":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "bci_week_var":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "bci_month_sum":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "bci_month_change":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "bci_month_ave":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "bci_month_var":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "weibo_day_last":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "weibo_week_num":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "weibo_month_num":{
                        "type": "long",
                        "index": "not_analyzed"
                    },
                    "update_time":{
                        "type": "string",
                        "index": "not_analyzed"
                    }
                }
            }
        }
    }
    exist_es = es.indices.exists(index="bci_history")
    if not exist_es:
        es.indices.create(index="bci_history", body=index_info, ignore=400)
    print exist_es

if __name__ == "__main__":
    print bci_history_mapping()
