#!/bin/sh
tmux kill-session -t flow4
tmux new-session -s flow4 -d

tmux new-window -n first -t flow4
tmux new-window -n vent -t flow4
tmux new-window -n work1 -t flow4
tmux new-window -n work2 -t flow4
tmux new-window -n work3 -t flow4
tmux new-window -n work4 -t flow4
tmux send-keys -t flow4:work1 'python zmq_work_weibo_flow4.py' C-m
tmux send-keys -t flow4:work2 'python zmq_work_weibo_flow4.py' C-m
tmux send-keys -t flow4:work3 'python zmq_work_weibo_flow4.py' C-m
tmux send-keys -t flow4:work4 'python zmq_work_weibo_flow4.py' C-m



