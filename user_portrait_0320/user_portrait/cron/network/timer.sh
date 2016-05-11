#!/bin/sh
tmux kill-session -t network

tmux new-session -s network -d
tmux new-window -n first -t network
tmux new-window -n second -t network
tmux send-keys -t network:second 'export SPARK_HOME="/home/spark/spark-1.3.0-bin-hadoop2.4"' C-m
tmux send-keys -t network:second 'echo $SPARK_HOME >> /home/log/network.log' C-m
tmux send-keys -t network:second 'ulimit -HSn 65536' C-m
tmux send-keys -t network:second 'ulimit -n >>  /home/log/network.log' C-m
tmux send-keys -t network:second 'python network.py >> /home/log/network.log' C-m
