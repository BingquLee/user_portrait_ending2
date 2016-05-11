#!/bin/sh
cd /home/user_portrait_0320/revised_user_portrait/user_portrait/user_portrait/cron/flow4
tmux kill-session -t flow4

tmux new-session -s flow4 -d
tmux new-window -n first -t flow4
tmux new-window -n redis -t flow4
tmux new-window -n es1 -t flow4
tmux new-window -n es2 -t flow4
tmux new-window -n es3 -t flow4
tmux new-window -n es4 -t flow4

tmux send-keys -t flow4:redis 'python scan_domain2sentiment.py' C-m
tmux send-keys -t flow4:es1 'python scan_topic2sentiment.py' C-m

