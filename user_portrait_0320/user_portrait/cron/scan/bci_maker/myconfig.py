import redis
import time
from bci_history_mapping import bci_history_mapping
from time_utils import ts2datetime, datetime2ts

BCIHIS_INDEX_NAME = 'bci_history'
BCIHIS_INDEX_TYPE = 'bci'

MAX_ITEMS = 1000
TOTAL_NUM = "total_number"
TODAY_BCI = "user_index"

BCI_INDEX_NAME_PRE = 'bci_'
BCI_INDEX_TYPE = 'bci'

REDIS_TEXT_BCI_HOST = '10.128.55.67'
REDIS_TEXT_BCI_PORT = '6379'

TODAY_TIME = ts2datetime(time.time())
r_flow = redis.StrictRedis(host=REDIS_TEXT_BCI_HOST, port=REDIS_TEXT_BCI_PORT, db=2)
