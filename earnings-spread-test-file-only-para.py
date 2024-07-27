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
import numpy as np
import multiprocessing as mp
from functools import partial
from collections import deque
import itertools

resultColumns = ['symbol', 'event', 'purchaseDate', 'sellDate',
                 'estimatedEPSThisYear', 'actualEPSLastYear',
                 'purchasePrice', 'sellPrice', 'thirtyDayVol']


def logit(message, logfile):
    print(message)
    logfile.write(f'{message}\n')


def workdays(d, end, excluded=(6, 7)):
    days = []
    while d.date() <= end.date():
        if d.isoweekday() not in excluded:
            days.append(d)
        d += timedelta(days=1)
    return days


def getCompanyEarnings(ticker):
    filename = f"./earnings/{ticker}.json"
    if exists(filename):
        data = json.load(open(filename))
        return list(reversed(data))
    return None


def getStockPrice(ticker):
    filename = f"./stock-price/{ticker}.json"
    if exists(filename):
        data = json.load(open(filename))
        return data
    return None


def getDate(date):
    return datetime.strptime(date, '%Y-%m-%d')


def getClosestOpenDate(allPricing, date):
    # If we land on a weekend, check the next two days until we get valid data
    try:
        newDate = datetime.strftime(date, '%Y-%m-%d')
        return allPricing[newDate], newDate
    except KeyError as e:
        try:
            newDate = datetime.strftime(date + timedelta(days=1), '%Y-%m-%d')
            return allPricing[newDate], newDate
        except KeyError as e:
            try:
                newDate = datetime.strftime(date + timedelta(days=2), '%Y-%m-%d')
                return allPricing[newDate], newDate
            except KeyError as e:
                newDate = datetime.strftime(date + timedelta(days=3), '%Y-%m-%d')
                return allPricing[newDate], newDate


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


def calculate_sma(start_date, allPricing, n=50):
    # Get all pricing from the n days before this
    start = start_date - timedelta(days=n*3)
    applicablePricing = []
    breakout = 0
    while len(applicablePricing) < n*2:
        breakout += 1
        try:
            d = datetime.strftime(start + timedelta(days=breakout), '%Y-%m-%d')
            p = allPricing[d]
            p['date'] = d
            applicablePricing.append(p)
        except KeyError as e:
            continue
        if breakout > 400:
            break

    iterable = list(map(lambda x: x['4. close'], applicablePricing))
    prices = pd.DataFrame(iterable)
    prices['ma'] = prices.rolling(n).mean()
    return list(prices[prices['ma'] > 0]['ma'])
    # # http://en.wikipedia.org/wiki/Moving_average
    # it = iter(iterable)
    # # create an iterable object from input argument
    # d = deque(itertools.islice(it, n - 1))
    # # create deque object by slicing iterable
    # d.appendleft(0)
    # s = sum(d)
    # for elem in it:
    #     s += elem - d.popleft()
    #     d.append(elem)
    #     yield s / n

# FOR EVAL-ING CURRENT OPPORTUNITIES
# First get earnings calendar for company earnings announcements in the 4-8 week range (AV)
# For each company in the list, get the EPS from last year (using FinnHub)
# Calculate whether the company is a candidate

# FOR BACKTRACKING
# First get earnings calendar for company earnings announcements in the next 3 months (AV)
## This is a heuristic, just to have a list of companies to start with
# For each company in the list, get the past earnings data from AV
# Run test below on each quarter, starting with earliest quarter

# To evaluate a stock from the past we need:
# - Pick a quarter
# - for that quarter, get the EPS from same quarter last year and compare to estimate from this year
# - What criteria do we compare against? EPS delta
# - Check the stock price for each day from X weeks before earnings, and track the sale once 2% increase

# PARAMETERS
DEBUG = False
PRODUCE_RESULTS = True
epsDeltaDollar = 1
# User percentages for EPS delta
useDeltaPerc = True
ignoreNegEps = True
stockPriceCutoff = 8
volatilityLower = .015
volatilityUpper = 1000
positiveSMA = True

#['symbol', 'name', 'reportDate', 'fiscalDateEnding', 'estimate', 'currency']
# companies = getEarningsCandidates()
# with open('earnings-12-99-99.json', "w") as outfile:
#     json.dump(companies, outfile, indent=4)
companies = json.load(open('./earnings-12-3-21.json'))


def get_next_year_quarter_data(earnings, quarter):
    nextYearQuarter = quarter + timedelta(days=365)
    nextYearQuarterData = list(filter(
        lambda x: abs((getDate(x['fiscalDateEnding']) - nextYearQuarter).days) < 2, earnings))
    return nextYearQuarterData

def run_company(company, gainRoi, lossCutOff, weekWindow, epsDeltaPerc):
    gains = 0
    losses = 0
    failsafes = 0
    file_results = pd.DataFrame(columns=resultColumns)
    ticker = company[0]
    try:

        earnings = getCompanyEarnings(ticker)
        if earnings is None:
            # We have no data from API
            return 0, 0, 0, file_results

        #print(f'Tracking: {ticker} | {company[1]}')

        for i, data in enumerate(earnings):
            # Invert the check, move to the future, not look to the past
            quarter = getDate(data['fiscalDateEnding'])
            try:
                lastYearQuarterActualEPS = data['reportedEPS']
                # If there is no EPS data, skip
                if lastYearQuarterActualEPS == 'None':
                    continue

                nextYearQuarterData = get_next_year_quarter_data(earnings, quarter)
                if len(nextYearQuarterData) == 0:
                    if DEBUG:
                        print(f'no future quarter to track for {ticker}: {quarter}')
                    continue

                nextYearEstimatedEPS = nextYearQuarterData[0]['estimatedEPS']
                allPricing = getStockPrice(ticker)
                if allPricing is None:
                    # If there is no Stock data, skip
                    continue

                # Run the compare
                try:
                    eEPS = float(nextYearEstimatedEPS)
                except ValueError:
                    continue

                aEPS = float(lastYearQuarterActualEPS)
                if float(aEPS) == 0.0:
                    if DEBUG:
                        print(f'Actual EPS is zero, cannot get delta. {quarter}')
                    continue
                if ignoreNegEps and (eEPS < 0 or aEPS < 0):
                    if DEBUG:
                        print(f'EPS or eEPS is negative, ignoring. {quarter}')
                    continue
                # Which calculation method
                if useDeltaPerc:
                    epsDiff = float(eEPS) - float(aEPS)
                    epsDelta = epsDiff / float(aEPS)
                    if epsDelta < epsDeltaPerc:
                        if DEBUG:
                            print(f'EPS delta too small, not tracking for quarter {quarter}')
                        continue
                else:
                    # use dollar amount
                    epsDelta = float(eEPS) - float(aEPS)
                    if epsDelta < epsDeltaDollar:
                        if DEBUG:
                            print(f'EPS delta too small, not tracking for quarter {quarter}')
                        continue

                # get price X weeks before the report date
                priceTargetDate = quarter - timedelta(days=weekWindow)
                try:
                    openPricing, purchaseDate = getClosestOpenDate(allPricing, priceTargetDate)
                except Exception:
                    continue

                # We are purchasing the stock!!
                purchasePrice = float(openPricing['1. open'])

                # ignore low priced stocks
                if purchasePrice < stockPriceCutoff:
                    continue

                volatility = calculate_volatility(allPricing, getDate(purchaseDate))
                if volatility < volatilityLower or volatility > volatilityUpper:
                    continue

                # Calculate and test Simple Moving Average (Rolling mean)
                sma = calculate_sma(getDate(purchaseDate), allPricing)
                df = pd.DataFrame(sma).reset_index()
                trend = np.polyfit(df['index'], df[0], deg=1)
                # if slope of trend is less than 0, skip this stock
                if positiveSMA and trend[0] < 0:
                    continue

                # Now loop the next X weeks of days to find a X% gain for sale
                for i in range(weekWindow):
                    track = i + 1
                    possibleSellDate = priceTargetDate + timedelta(days=track)
                    try:
                        priceDate = allPricing[datetime.strftime(possibleSellDate, '%Y-%m-%d')]
                    except KeyError as e:
                        # Market closed
                        continue
                    currentPrice = float(priceDate['2. high'])
                    # is this a gain within our boundary?
                    diff = currentPrice - purchasePrice
                    roi = diff / purchasePrice
                    # get the actual sale price with a 2% limit order
                    priceDelta = (purchasePrice * gainRoi)
                    # if the delta between the buy and sell date is less than
                    # the resulting dollar amount of the roi switch being less
                    # than one cent, sell at the next cent marker.
                    # ex. stock price $0.49, roi delta 2%, difference $0.0098 (less than one cent)
                    if priceDelta < 0.01:
                        priceDelta = 0.01

                    salePriceTarget = round(purchasePrice + priceDelta, 2)
                    if roi > gainRoi:
                        if PRODUCE_RESULTS:
                            file_results = file_results.append(pd.DataFrame(
                                [[ticker, 'gain', purchaseDate, possibleSellDate, eEPS,
                                  aEPS, purchasePrice, salePriceTarget, volatility]],
                                columns=resultColumns
                            ))
                        gains += 1
                        if DEBUG:
                            print(f'Successful sale in {quarter} for ${purchasePrice} -> ${salePriceTarget}')
                        break
                    if roi < (lossCutOff * -1):
                        if PRODUCE_RESULTS:
                            file_results = file_results.append(pd.DataFrame(
                                [[ticker, 'loss', purchaseDate, possibleSellDate, eEPS,
                                  aEPS, purchasePrice, currentPrice, volatility]],
                                columns=resultColumns
                            ))
                        losses += 1
                        if DEBUG:
                            print(f'Drop sale at {round(lossCutOff * 100, 0)}% in {quarter} from ${purchasePrice} -> ${salePriceTarget}')
                        break
                    # if last day before earnings, sell
                    if track + 1 == weekWindow:
                        diff = currentPrice - purchasePrice
                        pl = diff / purchasePrice
                        if PRODUCE_RESULTS:
                            file_results = file_results.append(pd.DataFrame(
                                [[ticker, 'failsafe', purchaseDate, possibleSellDate, eEPS,
                                  aEPS, purchasePrice, currentPrice, volatility]],
                                columns=resultColumns
                            ))
                        failsafes += 1
                        if DEBUG:
                            print(f'Failsafe sale in {quarter}: '
                                  f'realized diff (p: ${purchasePrice}, c: ${currentPrice}) | '
                                  f'{round(pl, 4) * 100}%')
            except Exception as e:
                print(f'{ticker} failed for quarter {quarter}: | {e}')
    except Exception as e:
        print(f'{ticker} failed: | {e}')
    return gains, losses, failsafes, file_results


def run_companies(params):
    try:
        companies = params[0]
        gainRoi = params[1]
        lossCutOff = params[2]
        weekWindow = params[3]
        epsDeltaPerc = params[4]
        timestamp = time.time()

        gainFormatted = round(gainRoi * 100, 1)
        lossFormatted = round(lossCutOff * 100,1)
        code = f'g{gainFormatted}-l{lossFormatted}-w{weekWindow}-e{round(epsDeltaPerc * 100,0)}'
        print(f'Code: {code}')
        log = open(f"runlog-{code}.txt", "a")

        results = []
        for idx, company in enumerate(companies):
            if idx % 100 == 0:
                print(f'Tracking: {company[0]} | {company[1]}')
            results.append(run_company(company, gainRoi, lossCutOff, weekWindow, epsDeltaPerc))

        gains = sum(map(lambda x: x[0], results))
        losses = sum(map(lambda x: x[1], results))
        failsafes = sum(map(lambda x: x[2], results))
        file_results = pd.DataFrame(columns=resultColumns)
        for result in results:
            if len(result[3]) > 0:
                file_results = file_results.append(result[3])

        if PRODUCE_RESULTS:
            file_results.to_csv(f'results/file-results-{code}-{timestamp}.csv')

        deltaPrint = f'{epsDeltaPerc * 100}%' if useDeltaPerc else f'${epsDeltaDollar}'
        #print(f'Ratios for {deltaPrint} EPS delta and {gainFormatted}% gain {lossFormatted}% loss with a {weekWindow / 7} week window')
        logit(f'Gains | Losses | Failsafes | {params[1:]} | {round((losses + failsafes) / (gains + losses + failsafes) * 100,2)}%', log)
        logit(f'{gains} | {losses} | {failsafes}', log)
        log.close()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    paramList = [
        (companies, .025, 0.06, 52, 0.25),
        (companies, .025, 0.06, 52, 0.50),
        (companies, .025, 0.04, 52, 0.50),
        (companies, .025, 0.04, 52, 0.25),

    ]

    print('Starting Run')
    pool = mp.Pool(mp.cpu_count())
    #pool = mp.Pool(1)

    results = pool.map(run_companies, [row for row in paramList])

    # Run one company on 12 threads
    # try:
    #     companies = paramList[0][0]
    #     gainRoi = paramList[0][1]
    #     lossCutOff = paramList[0][2]
    #     weekWindow = paramList[0][3]
    #     epsDeltaPerc = paramList[0][4]
    #     timestamp = time.time()
    #
    #     gainFormatted = round(gainRoi * 100, 1)
    #     lossFormatted = round(lossCutOff * 100,1)
    #     code = f'g{gainFormatted}-l{lossFormatted}-w{weekWindow}-e{round(epsDeltaPerc * 100,0)}'
    #     print(f'Code: {code}')
    #     log = open(f"runlog-{code}.txt", "a")
    #
    #     results = pool.map(run_company, [(row, gainRoi, lossCutOff, weekWindow, epsDeltaPerc) for row in companies])
    #
    #     gains = sum(map(lambda x: x[0], results))
    #     losses = sum(map(lambda x: x[1], results))
    #     failsafes = sum(map(lambda x: x[2], results))
    #     file_results = pd.DataFrame(columns=resultColumns)
    #     for result in results:
    #         if len(result[3]) > 0:
    #             file_results = file_results.append(result[3])
    #
    #     if PRODUCE_RESULTS:
    #         file_results.to_csv(f'results/file-results-{code}-{timestamp}.csv')
    #
    #     deltaPrint = f'{epsDeltaPerc * 100}%' if useDeltaPerc else f'${epsDeltaDollar}'
    #     #print(f'Ratios for {deltaPrint} EPS delta and {gainFormatted}% gain {lossFormatted}% loss with a {weekWindow / 7} week window')
    #     logit(f'Gains | Losses | Failsafes | {paramList[1:]} | {round((losses + failsafes) / (gains + losses + failsafes) * 100,2)}%', log)
    #     logit(f'{gains} | {losses} | {failsafes}', log)
    #     log.close()
    # except Exception as e:
    #     print(e)

    pool.close()
    pool.join()


