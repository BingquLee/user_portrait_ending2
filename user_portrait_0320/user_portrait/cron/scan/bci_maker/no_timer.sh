#!/bin/sh
cd /home/user_portrait_0320/revised_user_portrait/user_portrait/user_portrait/cron/scan/bci_maker

tmux kill-session -t bci_all
tmux new-session -s bci_all -d

tmux  new-window -n first -t bci_all
tmux  new-window -n redis -t bci_all
tmux new-window -n es1 -t bci_all
tmux new-window -n es2 -t bci_all
tmux new-window -n es3 -t bci_all
tmux new-window -n es4 -t bci_all

tmux send-keys -t bci_all:redis 'python update_no_bci_mapper.py >> /home/log/all_bci.log' C-m
tmux send-keys -t bci_all:es1 'python bci_history_reducer.py >> /home/log/all_bci.log' C-m
tmux send-keys -t bci_all:es2 'python bci_history_reducer.py >> /home/log/all_bci.log' C-m 
tmux send-keys -t bci_all:es3 'python bci_history_reducer.py >> /home/log/all_bci.log' C-m 
tmux send-keys -t bci_all:es4 'python bci_history_reducer.py >> /home/log/all_bci.log' C-m 

