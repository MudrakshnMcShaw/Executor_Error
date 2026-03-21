#!/bin/bash

# Find pm2 location dynamically
PM2=$(which pm2 2>/dev/null)

# If which doesn't work (no PATH set), check common locations
if [ -z "$PM2" ]; then
    if [ -f "/usr/local/bin/pm2" ]; then
        PM2="/usr/local/bin/pm2"
    elif [ -f "/usr/bin/pm2" ]; then
        PM2="/usr/bin/pm2"
    elif [ -f "$HOME/.npm-global/bin/pm2" ]; then
        PM2="$HOME/.npm-global/bin/pm2"
    else
        echo "Error: pm2 not found"
        exit 1
    fi
fi


cd "/root/Executor_Error/"
$PM2 delete Exec_Error
$PM2 start 'exec_error.py' --interpreter='/root/Executor_Error/venv/bin/python3' --namespace='Exec_Error' --name='Exec_Error_1' --time -- Exec_Error Exec_Error_1
$PM2 start 'exec_error.py' --interpreter='/root/Executor_Error/venv/bin/python3' --namespace='Exec_Error' --name='Exec_Error_2' --time -- Exec_Error Exec_Error_2
$PM2 start 'exec_error.py' --interpreter='/root/Executor_Error/venv/bin/python3' --namespace='Exec_Error' --name='Exec_Error_3' --time -- Exec_Error Exec_Error_3
$PM2 start 'exec_error.py' --interpreter='/root/Executor_Error/venv/bin/python3' --namespace='Exec_Error' --name='Exec_Error_4' --time -- Exec_Error Exec_Error_4