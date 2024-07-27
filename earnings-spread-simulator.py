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
import multiprocessing as mp
import os, fnmatch
import psycopg2
import psycopg2.extras
import itertools


def getDate(date):
    return datetime.strptime(date, '%Y-%m-%d')

def formatDate(date):
    return datetime.strftime(date, '%Y-%m-%d')


conn = psycopg2.connect("")
columns = ['id', 'symbol', 'purchaseDate', 'purchasePrice', 'sellDate', 'sellPrice', 'shares']

# PARAMETERS
DEBUG = False
timestamp = time.time()


def logit(message, logfile):
    print(message)
    logfile.write(f'{message}\n')


def build_id(symbol, purchase, shares):
    return f'{symbol}.{purchase}.{shares}'


def build_df():
    activeInvestments = pd.DataFrame(columns=columns)
    activeInvestments['sellDate'] = pd.to_datetime(activeInvestments['sellDate'], format='%Y-%m-%d')
    return activeInvestments


# ,symbol,event,purchaseDate,sellDate,estimatedEPSThisYear,actualEPSLastYear,purchasePrice,sellPrice
def run_simulation_for_year(eventsData, year, code, investment, gainCutoff):
    investmentPerStock = investment
    yearlyRunningTotal = 0
    yearlyBasis = 0
    investmentsMade = 0
    losses = 0

    log = open(f"simlogs/sim-log-{code}--{year}.txt", "a")

    print('Starting Run')
    start = datetime(year, 1, 1)
    end = start + timedelta(days = 365)

    thisYearsEvents = eventsData[
        (eventsData['purchaseDate'].dt.strftime('%Y-%m-%d') >= formatDate(start)) &
        (eventsData['purchaseDate'].dt.strftime('%Y-%m-%d') <= formatDate(end))
    ]

    for event in thisYearsEvents.iterrows():
        e = event[1]

        # If this events sellDate is in next year, ignore it
        # if e['sellDate'].year != year:
        #     print(f'Skipping stock {e["symbol"]} because sell date is next year')
        #     continue

        purchasePrice = e["purchasePrice"]
        shares = math.floor(investmentPerStock / purchasePrice)
        purchaseCost = shares * purchasePrice

        sellPrice = e["sellPrice"]
        proceeds = sellPrice * shares

        # track purchase cost in order to get total invested
        yearlyBasis += purchaseCost
        yearlyRunningTotal += proceeds

        investmentsMade += 1
        if proceeds - purchaseCost < 0:
            losses += 1

    logit(f'Total Investments: {investmentsMade} | Invested: {yearlyBasis}\n', log)
    logit(f'Total year return: ${round(yearlyRunningTotal, 2)}\n', log)
    return year, yearlyRunningTotal, yearlyBasis, investmentsMade, losses


def segregate_years(eventsData):
    start_date = eventsData.head()['purchaseDate']
    start_year = start_date.dt.date.to_numpy()[0].year
    years = []
    diff = 2021 - start_year
    for i, year in enumerate(range(diff)):
        years.append(start_year + i)
    return years


def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result


def run_files():
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    investment = 400
    # Read results file and sort by date
    # find files
    files = find(f'sim-data-*.csv', './results')
    groups = itertools.groupby(files, lambda x: x.split('-')[2])

    for group, results in groups:
        rawData = list(results)
        if len(rawData) < 3:
            continue
        eventsData = pd.read_csv(rawData[0])
        eventsData2 = pd.read_csv(rawData[1])
        eventsData3 = pd.read_csv(rawData[2])
        eventsData = eventsData.append(eventsData2).append(eventsData3)

        eventsData['purchaseDate'] = pd.to_datetime(eventsData['purchaseDate'], format='%Y-%m-%d')
        eventsData['sellDate'] = pd.to_datetime(eventsData['sellDate'], format='%Y-%m-%d')

        eventsData.sort_values(by=['purchaseDate'], inplace=True)

        # pull code from file name
        resultid = rawData[0].split('-')[2]

        cur.execute('SELECT * FROM simulation_results where result_run_id = %s',
                    (resultid,))
        sim = cur.fetchone()
        if sim is not None:
            continue

        cur.execute('SELECT * FROM result_runs where id = %s',
                    (resultid,))
        run = cur.fetchone()
        #gainCutoff = run['gain_cutoff']

        years = segregate_years(eventsData)
        output = []
        for year in years:
            out = run_simulation_for_year(eventsData, year, resultid, investment, None)
            returns = round(out[1], 2)
            yearlyBasis = round(out[2], 2)
            gainEvents = out[3] - out[4]
            roi = round(returns - yearlyBasis, 2)
            output.append(out)
            cur.execute("INSERT INTO simulation_results "
                        "(result_run_id, year, capital_invested_per_event, total_return_year_end, "
                        "total_gain_events, total_loss_events, capital_invested, roi) "
                        "VALUES (%s, %s, %s, %s, "
                        "%s, %s, %s, %s)",
                        (resultid, year, investment, returns,
                         gainEvents, out[4], yearlyBasis, roi))

            conn.commit()

        print(output)


run_files()
