#!/bin/bash

SESSION_NAME="monitor"

# Start a new tmux session in detached mode
tmux new-session -d -s $SESSION_NAME

# Split the window vertically (top/bottom)
tmux split-window -v

# Run nvidia-smi in the left pane
tmux send-keys -t $SESSION_NAME:0.0 "watch -n 1 nvidia-smi" C-m

# Run htop in the right pane
tmux send-keys -t $SESSION_NAME:0.1 "htop" C-m

# Attach to the tmux session
tmux attach-session -t $SESSION_NAME
