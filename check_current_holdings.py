import json
import requests
from datetime import datetime, timedelta
import time
import os
import psycopg2
import psycopg2.extras
import math
import boto3
import logging
import sys
import random
import csv
import re
from bs4 import BeautifulSoup

# Setup
conn = psycopg2.connect("host=45.77.120.179 dbname=other user=other password=oDyK7fGjwqY6D")

alphaVantageApi = 'ORL7L6P47R2DYAS3'
stockPriceUrl = 'https://realstonks.p.rapidapi.com/'
avEarningsCalendar = f'https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&horizon=3month&apikey={alphaVantageApi}'
listingStatusUrl = f'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={alphaVantageApi}'

def parse_date(dateString):
    return datetime.strptime(dateString, '%Y-%m-%d')


def format_date(date):
    return '%d-%02d-%02d' % (date.year, date.month, date.day)


def format_datetime(date):
    return '%d-%02d-%02d-%02d-%02d-%02d' % \
           (date.year, date.month, date.day, date.hour, date.minute, date.second)


def get_stock_price(symbol):
    url = f'https://www.marketwatch.com/investing/stock/{symbol}'
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.90 Safari/537.36'}
    r = requests.get(url, headers=headers)
    html =  BeautifulSoup(r.text, 'html.parser')
    time.sleep(1)
    # get the estimate text, and determine if we have to check this one or not.
    return float(html.find_all(class_='intraday__price')[0].find_all('bg-quote')[0].text)


def get_pending_trades(cur):
    cur.execute('SELECT * FROM trader_sim_log WHERE sell_date is null',
                ())
    return cur.fetchall()

if __name__ == "__main__":
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print(f'Symbol | cost | current value | gain/loss | percent')
    holdings = get_pending_trades(cur)
    holdingsSort = sorted(holdings, key=lambda d: d['symbol']) 
    for holding in holdings:
        try:
            target_price = holding['target_sell_price']
            prempt_target_sell_price = holding['prempt_target_sell_price']
            symbol = holding['symbol']
            earnings_report = holding['earnings_report_date']
            shares = holding['shares']
            purchase_date = holding['purchase_date']
            purchase_price = holding['purchase_price']
            cost = holding['total_cost']
            try:
                try:
                    current_price = get_stock_price(symbol)
                except Exception as e:
                    # Wait 10 seconds on holding get fail to make sure that it's actually a failure.
                    time.sleep(10)
                    current_price = get_stock_price(symbol)
            except Exception as e:
                print(f'could not get current price for {symbol}')
                continue
            
            current_value = round(current_price * shares, 2)
            delta = current_value - cost
            roi = (delta / cost) * 100
            print(f'{symbol} | {cost} | {current_value} | {round(delta, 2)} | {round(roi,2)}%')
            
        except Exception as e:
            print(e)