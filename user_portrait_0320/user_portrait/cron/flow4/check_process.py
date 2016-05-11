# -*- coding:utf-8 -*-

import subprocess
import sys
import os
import time

restart_cmd = './flow4.sh'
def check(p_name):
    cmd = 'ps -ef|grep %s|grep -v "grep"' % p_name
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    if p.wait() == 0:
        val = p.stdout.read()
        if p_name in val:
            return "running"
    else:
        #os.system("python ./%s &" % p_name)
        q = subprocess.Popen(restart_cmd, shell=True, stdout=subprocess.PIPE)
        return "restart"


if __name__ == '__main__':

    # test procedure running
    d_name = 'zmq_work_weibo_flow4.py'
    result = check(d_name)

    ts = str(int(time.time()))
    print_log = "&".join([d_name, result, ts])
    print print_log


