#!/bin/bash

cd "/root/Executor_Error/"
pm2 delete Exec_Error
pm2 start 'exec_error.py' --interpreter='/root/Executor_Error/venv/bin/python3' --namespace='Exec_Error' --name='Exec_Error_1' --time -- Exec_Error Exec_Error_1
pm2 start 'exec_error.py' --interpreter='/root/Executor_Error/venv/bin/python3' --namespace='Exec_Error' --name='Exec_Error_2' --time -- Exec_Error Exec_Error_2
pm2 start 'exec_error.py' --interpreter='/root/Executor_Error/venv/bin/python3' --namespace='Exec_Error' --name='Exec_Error_3' --time -- Exec_Error Exec_Error_3
pm2 start 'exec_error.py' --interpreter='/root/Executor_Error/venv/bin/python3' --namespace='Exec_Error' --name='Exec_Error_4' --time -- Exec_Error Exec_Error_4