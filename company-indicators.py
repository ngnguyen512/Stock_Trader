import json
import requests
from collections import OrderedDict
import math
import csv
import random
from datetime import datetime, date, timedelta
import time
from os.path import exists
import pandas as pd
import statistics

DEBUG = False
apikey = 'SWJCT9RBZM8S1KBE'
apikey2 = '0CYPLFS28TMF2YXY'
apikey3 = '1CZB8UJLQ55F6L1A'
simpleMovingAverageUrl = f'https://www.alphavantage.co/query?function=SMA&interval=weekly&time_period=50&series_type=open&symbol={{0}}&apikey={{1}}'
earningsCalendarUrl = f'https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&horizon=3month&apikey={{1}}'
stockPriceUrl = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&outputsize=full&symbol={{0}}&apikey={{1}}'
api = 1


def getDate(date):
    return datetime.strptime(date, '%Y-%m-%d')


def get_trend(p):
    p_diff = []
    for p_index in range(len(p)):
        if p_index != len(p)-1:
            p_diff.append(p[p_index+1]-p[p_index])

    trend = statistics.mean(p_diff)
    return trend


def getMovingAverage(ticker):
    if api == 1:
        u = simpleMovingAverageUrl.format(ticker, apikey)
    elif api == 2:
        u = simpleMovingAverageUrl.format(ticker, apikey2)
    else:
        u = simpleMovingAverageUrl.format(ticker, apikey3)

    r = requests.get(u)
    data = r.json()

    items = data['Technical Analysis: SMA']
    return items


companies = [
'MSFT',
'MU',
'NKE',
'NVDA',
'PAYX',
'PKG',
'PVH',
'QCOM',
'REVG',
'ROKU',
'RUN',
'SHOP',
'SM',
'SMPL',
'SMTC',
'SNA',
'SYY',
'TGT',
'TLYS',
'TOL',
'TSLA',
'TTC',
'UNH',
'URBN',
'VOO',
'WGO',
'WSM',
'XLF',
'ZG',
]

try:
    for k, company in enumerate(companies):
        ticker = company

        #print(f'Tracking: {ticker}')

        maData = getMovingAverage(ticker)
        if len(maData) == 0:
            # We have no data from API
            continue

        maSet = []
        for i, data in enumerate(maData):
            start_date = datetime.now() - timedelta(days=365)
            if getDate(data) > start_date:
                maSet.append(float(maData[data]["SMA"]))

        t = get_trend(list(reversed(maSet)))
        print(f'{ticker}: {t}')

        time.sleep(12)

finally:
    print('done')




