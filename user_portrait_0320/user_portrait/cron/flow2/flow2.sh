#!/bin/sh
tmux kill-session -t flow2
tmux new-session -s flow2 -d

tmux new-window -n first -t flow2
tmux new-window -n vent -t flow2
tmux new-window -n work1 -t flow2
tmux new-window -n work2 -t flow2
tmux new-window -n work3 -t flow2
tmux new-window -n work4 -t flow2
tmux send-keys -t flow2:work1 'python zmq_work_weibo_flow2.py' C-m
tmux send-keys -t flow2:work2 'python zmq_work_weibo_flow2.py' C-m
tmux send-keys -t flow2:work3 'python zmq_work_weibo_flow2.py' C-m
tmux send-keys -t flow2:work4 'python zmq_work_weibo_flow2.py' C-m



