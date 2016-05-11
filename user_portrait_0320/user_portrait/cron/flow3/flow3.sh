#!/bin/sh
tmux kill-session -t flow3
tmux new-session -s flow3 -d

tmux new-window -n first -t flow3
tmux new-window -n vent -t flow3
tmux new-window -n work1 -t flow3
tmux new-window -n work2 -t flow3
tmux new-window -n work3 -t flow3
tmux new-window -n work4 -t flow3
tmux send-keys -t flow3:work1 'python zmq_work_weibo_flow3.py' C-m
tmux send-keys -t flow3:work2 'python zmq_work_weibo_flow3.py' C-m
tmux send-keys -t flow3:work3 'python zmq_work_weibo_flow3.py' C-m
tmux send-keys -t flow3:work4 'python zmq_work_weibo_flow3.py' C-m



