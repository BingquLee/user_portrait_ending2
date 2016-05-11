# -*- coding:utf-8 -*-

import subprocess
import sys
import os
import time

restart_cmd = './../flow1_work.sh'

def check(p_name):
    cmd = 'ps -ef|grep %s|grep -v "grep"' % p_name
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    if p.wait() == 0:
        val = p.stdout.read()
        #print val
        if p_name in val:
            return "running"
            #print "%s %s running" % (time.ctime(),p_name)
    else:
        #os.system("python ./%s &" % p_name)
        q = subprocess.Popen(restart_cmd, shell=True, stdout=subprocess.PIPE)
        return "restart"
        #print "%s %s restart" % (time.ctime(), p_name)


if __name__ == '__main__':
    # 查询zmq_vent_weibo.py是否在执行
    d_name = 'zmq_work_weibo.py'
    result = check(d_name)

    ts = str(int(time.time()))
    print_log = "&".join([d_name, result, ts])
    print print_log

