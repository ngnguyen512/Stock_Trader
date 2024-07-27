#!/bin/bash

cd ~/opt/stock-trader

git pull origin master

source venv/bin/activate

python3 run-market-watcher.py
