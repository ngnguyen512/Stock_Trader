import ujson
import traceback
import sys
import requests
from collections import OrderedDict
import math
import csv
import random
from datetime import datetime, date, timedelta
import time
from os.path import exists
import pandas as pd
import numpy as np
import multiprocessing as mp
import psycopg2
import psycopg2.extras
import re

conn = psycopg2.connect("host=45.77.120.179 dbname=other user=other password=F3X3q7h8irUK")
resultColumns = ['symbol', 'event', 'year', 'purchaseDate', 'sellDate',
                 'estimatedEPSThisYear', 'actualEPSLastYear',
                 'purchasePrice', 'sellPrice', 'thirtyDayVol', 'sma',
                 'volu_multiple', 'volu_average', 'volu_current']

# PARAMETERS
DEBUG = False
PRODUCE_RESULTS = True


def logit(message, logfile):
    print(message)
    logfile.write(f'{message}\n')


def getCompanyEarnings(ticker):
    filename = f"./earnings/{ticker}.json"
    if exists(filename):
        data = ujson.loads(open(filename).read())
        if len(data) == 0:
            return None
        return list(reversed(data))
    return None


def getStockPrice(ticker):
    filename = f"./stock-price/{ticker}.json"
    if exists(filename):
        data = ujson.loads(open(filename).read())
        if len(data) == 0:
            return None
        return data
    return None


def loadMarketCaps():
    filename = f"./market_cap.json"
    if exists(filename):
        data = ujson.loads(open(filename).read())
        if len(data) == 0:
            return None
        return data
    return None


def getDate(input):
    return date.fromisoformat(input)


def formatDate(date):
    return '%d-%02d-%02d' % (date.year, date.month, date.day)


def getClosestOpenDate(allPricing, date):
    # If we land on a weekend, check the next two days until we get valid data
    try:
        newDate = date
        return allPricing[formatDate(newDate)], newDate
    except KeyError as e:
        try:
            newDate = date + timedelta(days=1)
            return allPricing[formatDate(newDate)], newDate
        except KeyError as e:
            try:
                newDate = date + timedelta(days=2)
                return allPricing[formatDate(newDate)], newDate
            except KeyError as e:
                newDate = date + timedelta(days=3)
                return allPricing[formatDate(newDate)], newDate


def getNearestOpenDatePast(allPricing, date):
    # If we land on a weekend, check the next two days until we get valid data
    try:
        newDate = formatDate(date - timedelta(days=1))
        return allPricing[newDate], newDate
    except KeyError as e:
        try:
            newDate = formatDate(date - timedelta(days=2))
            return allPricing[newDate], newDate
        except KeyError as e:
            newDate = formatDate(date - timedelta(days=3))
            return allPricing[newDate], newDate



def getLastNDaysVolume(allPricing, purchaseDate, period):
    # Start with the day before because we wont have current day volume until end of day
    start = purchaseDate
    daysChecked = 1
    dates = [(formatDate(start), allPricing[formatDate(start)]['5. volume'])]
    # add extra day so we can get day before purchase and still have 10 day history
    while len(dates) < period + 1:
        next = start - timedelta(days=daysChecked)
        daysChecked += 1
        # circuit breaker
        if daysChecked > 1000:
            return []
        if next.weekday() < 5:
            try:
                dates.append((formatDate(next), allPricing[formatDate(next)]['5. volume']))
            except KeyError as e:
                continue
    return dates


def calculate_volatility(allPricing, targetDate):
    # Get all pricing from the month before this
    start = targetDate - timedelta(days=30)
    applicablePricing = []
    for i in range(30):
        try:
            applicablePricing.append(allPricing[datetime.strftime(start + timedelta(days=i), '%Y-%m-%d')])
        except KeyError as e:
            continue
    # Now that we have pricing, calculate interday returns
    # divide the stock’s current closing price by the previous day’s closing price, then subtracting 1
    interdayReturns = []
    reversedPricing = list(reversed(applicablePricing))
    for idx, pricing in enumerate(reversedPricing):
        try:
            # stop at the item just before last, because calculation looks one ahead
            if idx + 2 == len(reversedPricing):
                break
            yesterday = reversedPricing[idx + 1]
            interdayreturn = (float(pricing['4. close']) - float(yesterday['4. close'])) - 1
            interdayReturns.append(interdayreturn)
        except Exception as e:
            print(f'volatility calc failed for index {idx} of total {len(reversedPricing)}')
            return 0
    # get standard deviation for all returns
    try:
        return np.std(interdayReturns, ddof=1)
    except Exception as e:
        print(e)
        return 0


def moving_average(items, n):
    ret = np.cumsum(items, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n - 1:] / n


def calculate_sma(start_date, allPricing, n=50):
    # Get all pricing from the n days before this
    start = start_date - timedelta(days=n*3)
    applicablePricing = []
    breakout = 0
    while len(applicablePricing) < n*2:
        breakout += 1
        try:
            d = formatDate(start + timedelta(days=breakout))
            applicablePricing.append(float(allPricing[d]['4. close']))
        except KeyError as e:
            continue
        if breakout > 400:
            break

    ma = moving_average(applicablePricing, n)
    return ma


def get_next_year_quarter_data(earnings, quarter):
    nextYearQuarterExact = quarter + timedelta(days=365)
    # got two days out in either direction until we find a date
    daysWindow = 3
    for i in range(daysWindow):
        forward = nextYearQuarterExact + timedelta(days=i)
        item = [n for n in earnings if n['fiscalDateEnding'] == formatDate(forward)]
        if len(item) > 0:
            return item
    # go into the past
    for j in range(daysWindow):
        behind = nextYearQuarterExact - timedelta(days=j)
        item = [n for n in earnings if n['fiscalDateEnding'] == formatDate(behind)]
        if len(item) > 0:
            return item
    return []


def getPriceBoundaries(purchasePrice, gainBoundary, lossBoundary):
    # get the actual sale price with a limit order
    gainPriceDelta = (purchasePrice * (gainBoundary / 100))
    lossPriceDelta = (purchasePrice * (lossBoundary / 100))
    # if the delta between the buy and sell date is less than
    # the resulting dollar amount of the roi switch being less
    # than one cent, sell at the next cent marker.
    # ex. stock price $0.49, roi delta 2%, difference $0.0098 (less than one cent)
    if gainPriceDelta < 0.01:
        gainPriceDelta = 0.01

    # We sell at soon as we have a % gain, not at the high. because of limit orders
    positiveSaleTarget = round(purchasePrice + gainPriceDelta, 2)
    negativeSaleTarget = round(purchasePrice + lossPriceDelta, 2)
    return positiveSaleTarget, negativeSaleTarget


def runEpsCompare(estimated, actual, ignoreNeg, deltaPercentage):
    eEPS = float(estimated)
    aEPS = float(actual)
    if float(aEPS) == 0.0:
        if DEBUG:
            print(f'Actual EPS is zero, cannot get delta.')
        return False
    if ignoreNeg and (eEPS < 0 or aEPS < 0):
        if DEBUG:
            print(f'EPS or eEPS is negative, ignoring.')
        return False
    epsDiff = float(eEPS) - float(aEPS)
    epsDelta = epsDiff / float(aEPS)
    if epsDelta < (deltaPercentage / 100):
        if DEBUG:
            print(f'EPS delta too small, not tracking for quarter.')
        return False
    return True


def safeGetEPSData(earningsObject, nextQuarterData):
    lastYearQuarterActualEPS = earningsObject['reportedEPS']
    # If there is no EPS data, skip
    if lastYearQuarterActualEPS == 'None':
        return 0,0
    if len(nextQuarterData) == 0:
        if DEBUG:
            print(f'no future quarter to track: {end}')
        return 0,0
    nextYearEstimatedEPS = nextQuarterData[0]['estimatedEPS']
    # If there is no EPS data, skip
    if nextYearEstimatedEPS == 'None':
        return 0,0
    return nextYearEstimatedEPS, lastYearQuarterActualEPS


def run_company(parameters):
    company = parameters[0]
    gainRoi = parameters[1]
    lossCutOff = parameters[2]
    weekWindow = parameters[3]
    epsDeltaPerc = parameters[4]
    ignoreNegEps = parameters[5]
    stockPriceCutoff = parameters[6]
    volatilityLower = parameters[7]
    volatilityUpper = parameters[8]
    positiveSMA = parameters[9]
    step_down_period = parameters[10]
    step_down_percentage = parameters[11]
    rvol_period = parameters[12]
    market_cap_floor = parameters[13]
    market_cap = parameters[14]

    gain_results = []
    loss_results = []
    fs_results = []
    ticker = company[0]
    allPricing = getStockPrice(ticker)

    if allPricing is None:
        # We have no data from API
        return gain_results, loss_results, fs_results

    earnings = getCompanyEarnings(ticker)
    if earnings is None:
        # We have no data from API
        return gain_results, loss_results, fs_results

    try:
        for ei, currentQuarter in enumerate(earnings):
            # Invert the check, move to the future, not look to the past
            quarterEnd = getDate(currentQuarter['fiscalDateEnding'])
            try:
                nextYearQuarterData = get_next_year_quarter_data(earnings, quarterEnd)
                # we want next years report date because that is when we are buying and testing against.
                # We only care about the past data for the YoY EPS data
                reportDate = getDate(nextYearQuarterData[0]['reportedDate'])

                nextYearEstimatedEPS, lastYearQuarterActualEPS = safeGetEPSData(currentQuarter, nextYearQuarterData)
                if nextYearEstimatedEPS == 0 and lastYearQuarterActualEPS == 0:
                    #print(f'for ticker: {ticker}')
                    continue
                ########################################################################################
                ### DO NOT USE `data` or `quarterEnd` FOR ANY FUNCTIONAL REQUIREMENT PAST THIS POINT ###
                ########################################################################################

                # Run the compare
                cont = runEpsCompare(nextYearEstimatedEPS, lastYearQuarterActualEPS, ignoreNegEps, epsDeltaPerc)
                if not cont:
                    #print(f'for quarter: {quarterEnd}')
                    continue

                # get price X weeks before the report date
                priceTargetDate = reportDate - timedelta(days=weekWindow)
                try:
                    openPricing, purchaseDate = getClosestOpenDate(allPricing, priceTargetDate)
                except Exception:
                    continue

                # We are purchasing the stock!!
                purchasePrice = float(openPricing['1. open'])
                year = purchaseDate.year

                # ignore low priced stocks
                if purchasePrice < stockPriceCutoff:
                    continue

                volatility = None
                if (volatilityLower != 0.0 and volatilityUpper != 0.0):
                    volatility = calculate_volatility(allPricing, purchaseDate)
                    if (volatility < volatilityLower or volatility > volatilityUpper):
                        continue

                trend = [None]
                try:
                    if positiveSMA:
                        # Calculate and test Simple Moving Average (Rolling mean)
                        sma = calculate_sma(purchaseDate, allPricing)
                        df = pd.DataFrame(sma).reset_index()
                        trend = np.polyfit(df['index'], df[0], deg=1)
                        # if slope of trend is less than 0, skip this stock
                        if trend[0] < 0:
                            continue
                except OverflowError as e:
                    print('SMA failed for future date')
                    # we tried to get a data in the future.
                    # silent failure is fine here

                # Are we calculating Relative Volume for this stock?
                vol_multiple = None
                vol_avg = None
                vol_curr = None
                if rvol_period is not None:
                    volumes = getLastNDaysVolume(allPricing, purchaseDate, rvol_period)
                    if len(volumes) >= rvol_period:
                        # first item is today's volume, second is yesterdays
                        today = int(volumes[0][1])
                        volume_ints = list(map(lambda x: int(x[1]), volumes[1:]))
                        vol_avg = sum(volume_ints) / len(volume_ints)
                        vol_curr = today
                        if vol_avg != 0:
                            vol_multiple = today / vol_avg

                if market_cap_floor is not None and market_cap_floor != 0:
                    comp_m_c = list(filter(lambda x: x['ticker'] == ticker, market_cap["ticker"]))
                    if len(comp_m_c) > 0:
                        raw = comp_m_c[0]['marketcap'].replace('$', '')
                        decimal = re.findall('\d*\.?\d+', raw)[0]
                        if 'T' in raw:
                            cap = float(decimal) * 1_000_000_000_000
                        elif 'B' in raw:
                            cap = float(decimal) * 1_000_000_000
                        elif 'M' in raw:
                            cap = float(decimal) * 1_000_000
                        else:
                            cap = float(decimal)
                        if cap < market_cap_floor:
                            continue
                    else:
                        print(f'no market cap data for {ticker}')

                # Now loop the next X weeks of days to find a X% gain for sale
                w = 0
                while w < weekWindow - 1:
                    w += 1
                    possibleSellDate = priceTargetDate + timedelta(days=w)
                    sellNow = False
                    try:
                        priceDate = allPricing[formatDate(possibleSellDate)]
                    except KeyError as e:
                        # Market closed
                        # If market close is too close to the earnings report, sell
                        if (possibleSellDate - timedelta(days=2)).weekday() >= 5:
                            sellNow = True
                        else:
                            continue
                    currentDayHigh = float(priceDate['2. high'])
                    currentDayLow = float(priceDate['3. low'])
                    currentDayClose = float(priceDate['4. close'])
                    # is this a gain or loss within our boundary?
                    highDiff = currentDayHigh - purchasePrice
                    lowDiff = currentDayLow - purchasePrice
                    # we have to track the high and low separately
                    # because with limit orders the loss/gain will be captured
                    # the second we pass the boundary
                    positiveroi = highDiff / purchasePrice
                    negativeroi = lowDiff / purchasePrice

                    # If we want to step down, then step down the gain cutoff
                    gainCutoff = gainRoi
                    if step_down_period is not None and step_down_percentage is not None:
                        # Period in days, percentage in decimal. 0.5 for half percent step down
                        triggerPeriod = track % step_down_period == 0
                        if triggerPeriod:
                            multipler = track / step_down_period
                            gainCutoff = gainRoi - round(step_down_percentage * multipler, 3)
                            # don't go below half a percent
                            if gainCutoff <= .5:
                                gainCutoff = .5
                    lossBoundary = (lossCutOff * -1)

                    positiveSaleTarget, negativeSaleTarget = getPriceBoundaries(purchasePrice, gainCutoff, lossBoundary)

                    if positiveroi >= (gainCutoff / 100):
                        row = [ticker, 'gain', year, purchaseDate, possibleSellDate, nextYearEstimatedEPS,
                              lastYearQuarterActualEPS, purchasePrice, positiveSaleTarget, volatility, trend[0],
                              vol_multiple, vol_avg, vol_curr]
                        gain_results.append(row)
                        if DEBUG:
                            print(f'Successful sale in {quarterEnd} for ${purchasePrice} -> ${positiveSaleTarget}')
                        break

                    if negativeroi <= (lossBoundary / 100):
                        row = [ticker, 'loss', year, purchaseDate, possibleSellDate, nextYearEstimatedEPS,
                              lastYearQuarterActualEPS, purchasePrice, negativeSaleTarget, volatility, trend[0],
                              vol_multiple, vol_avg, vol_curr]
                        loss_results.append(row)
                        if DEBUG:
                            print(f'Drop sale at {round(lossBoundary, 0)}% '
                                  f'in {quarterEnd} from ${purchasePrice} -> '
                                  f'${negativeSaleTarget}')
                        break
                    # if last day before earnings, sell as a failsafe
                    if w + 1 == weekWindow or sellNow:
                        # Check if we should try another quarter, only if we haven't done this yet.
                        if weekWindow < 90:
                            nextQuarter = earnings[ei + 1]
                            nexyQuarterEnd = getDate(nextQuarter['fiscalDateEnding'])
                            try:
                                nextQuarterData = get_next_year_quarter_data(earnings, nexyQuarterEnd)
                                # we want next years report date because that is when we are buying and testing against.
                                # We only care about the past data for the YoY EPS data
                                reportDate = getDate(nextQuarterData[0]['reportedDate'])
                                estimated, actual = safeGetEPSData(nextQuarter, nextQuarterData)
                                if estimated == 0 and actual == 0:
                                    # print(f'for ticker: {ticker}')
                                    continue
                                # Run the compare
                                cont = runEpsCompare(estimated, actual, ignoreNegEps,
                                                     epsDeltaPerc)
                                if not cont:
                                    # print(f'for quarter: {quarterEnd}')
                                    continue
                                # next quarter looks good, lets keep going.
                                # Set new weekwindow
                                weekWindow = weekWindow + 90
                                continue
                            except Exception as e:
                                print(f'{ticker} failed for quarter {nexyQuarterEnd}: | {e}')
                                traceback.print_exception(*sys.exc_info())

                        # minus from purchase to produce correct +/- values
                        # if diff is less than 0, we have a negative or a loss
                        diff = round(currentDayClose - purchasePrice, 4)
                        pl = diff / purchasePrice
                        row = [ticker, 'failsafe_p' if diff > 0 else 'failsafe_n', year, purchaseDate, possibleSellDate,
                               nextYearEstimatedEPS,
                               lastYearQuarterActualEPS, purchasePrice, currentDayClose, volatility, trend[0],
                               vol_multiple, vol_avg, vol_curr]
                        fs_results.append(row)
                        if DEBUG:
                            print(f'Failsafe sale in {quarterEnd}: '
                                  f'realized diff (p: ${purchasePrice}, c: ${currentDayHigh}) | '
                                  f'{round(pl, 4) * 100}%')
            except Exception as e:
                print(f'{ticker} failed for quarter {quarterEnd}: | {e}')
                traceback.print_exception(*sys.exc_info())
    except Exception as e:
        print(f'{ticker} failed: | {e}')
        traceback.print_exception(*sys.exc_info())
    return gain_results, loss_results, fs_results


if __name__ == "__main__":
    companies = ujson.loads(open('./earnings-12-3-21.json').read())
    market_cap = loadMarketCaps()
    # Get unrun scenarios from database
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    kill = False

    while not kill:
        start = time.time()
        print(f'Starting Run on {mp.cpu_count()}')

        cur.execute('SELECT * FROM result_runs where run_date is null and processing = false')
        #cur.execute('SELECT * FROM result_runs where id = \'3953\'')
        run = cur.fetchone()

        cur.execute("UPDATE result_runs set processing = true where id = %s",
                    (run['id'],))
        conn.commit()

        timestamp = time.time()

        pool = mp.Pool(mp.cpu_count())
        #pool = mp.Pool(1)

        results = pool.map(run_company,
                           [(row,
                             run['gain_cutoff'],
                             run['loss_cutoff'],
                             run['lookback_window'],
                             run['eps_delta'],
                             run['exclude_neg_eps'],
                             run['stock_price_limit'],
                             run['volatility_low'],
                             run['volatility_high'],
                             run['check_sma'],
                             run['step_down_period'],
                             run['step_down_percentage'],
                             run['rvol_period'],
                             run['market_cap_floor'],
                             market_cap) for row in companies])

        # results = []
        # for row in companies:
        #     results.append(run_company((row,
        #      run['gain_cutoff'],
        #      run['loss_cutoff'],
        #      run['lookback_window'],
        #      run['eps_delta'],
        #      run['exclude_neg_eps'],
        #      run['stock_price_limit'],
        #      run['volatility_low'],
        #      run['volatility_high'],
        #      run['check_sma'],
        #      run['step_down_period'],
        #      run['step_down_percentage'],
        #      run['rvol_period'])))

        # save results to DB
        file_results = pd.DataFrame(columns=resultColumns)
        gain_results = pd.DataFrame([val for sublist in [i[0] for i in results if i[0] != []] for val in sublist], columns=resultColumns)
        loss_results = pd.DataFrame([val for sublist in [i[1] for i in results if i[1] != []] for val in sublist], columns=resultColumns)
        flsf_results = pd.DataFrame([val for sublist in [i[2] for i in results if i[2] != []] for val in sublist], columns=resultColumns)

        gains = len(gain_results)
        losses = len(loss_results)
        failsafes_p = len(flsf_results[flsf_results['event'] == 'failsafe_p'])
        failsafes_n = len(flsf_results[flsf_results['event'] == 'failsafe_n'])

        if PRODUCE_RESULTS:
            gain_results.to_csv(f'results/sim-data-{run["id"]}-{timestamp}-g.csv')
            loss_results.to_csv(f'results/sim-data-{run["id"]}-{timestamp}-l.csv')
            flsf_results.to_csv(f'results/sim-data-{run["id"]}-{timestamp}-f.csv')

        cur.execute("UPDATE result_runs "
                    "SET res_gains = %s, res_losses = %s, res_failsafes_p = %s, "
                    "res_failsafes_n = %s, processing = false, run_date = now() "
                    "WHERE id = %s",
                    (gains, losses, failsafes_p, failsafes_n, run['id']))

        file_results = pd.concat([gain_results, loss_results, flsf_results], axis=0)
        years = file_results.groupby('year')
        for year,items in years:
            y_gains = len(items[items['event'] == 'gain'])
            y_losses = len(items[items['event'] == 'loss'])
            y_failsafes_p = len(items[items['event'] == 'failsafe_p'])
            y_failsafes_n = len(items[items['event'] == 'failsafe_n'])
            max_concurrent_investments = max(list(map(lambda x: len(x[1]),items.groupby('purchaseDate').groups.items())))
            cur.execute("INSERT INTO result_run_years "
                        "(result_run_id, year, gains, losses, "
                        "failsafes_p, failsafes_n, run_date, max_concurrent_investments) "
                        "VALUES (%s, %s, %s, %s, %s, %s, now(), %s)",
                        (run['id'], year, y_gains, y_losses,
                         y_failsafes_p, y_failsafes_n, max_concurrent_investments))

        conn.commit()

        pool.close()
        pool.join()
        end = time.time()
        print(end-start)




