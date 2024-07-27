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
import sys

resultColumns = ['symbol', 'event', 'purchaseDate', 'sellDate',
                 'estimatedEPSThisYear', 'actualEPSLastYear',
                 'purchasePrice', 'sellPrice']

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



#['symbol', 'name', 'reportDate', 'fiscalDateEnding', 'estimate', 'currency']
# companies = getEarningsCandidates()
# with open('earnings-12-99-99.json', "w") as outfile:
#     json.dump(companies, outfile, indent=4)
companies = json.load(open('./earnings-12-3-21.json'))
timestamp = time.time()


def run_company(company, results):
    gains = 0
    losses = 0
    try:
        ticker = company[0]

        earnings = getCompanyEarnings(ticker)
        if earnings is None:
            # We have no data from API
            return results

        print(f'Tracking: {ticker} | {company[1]}')

        for i, data in enumerate(earnings):
            # Invert the check, move to the future, not look to the past
            quarter = getDate(data['fiscalDateEnding'])
            lastYearQuarterActualEPS = data['reportedEPS']
            # If there is no EPS data, skip
            if lastYearQuarterActualEPS == 'None':
                continue

            nextYearQuarter = quarter + timedelta(days=365)
            nextYearQuarterData = list(filter(
                lambda x: abs((getDate(x['fiscalDateEnding']) - nextYearQuarter).days) < 2, earnings))
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
                openDate, purchaseDate = getClosestOpenDate(allPricing, priceTargetDate)
            except Exception:
                continue

            # We are purchasing the stock!!
            purchasePrice = float(openDate['1. open'])

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
                    results = results.append(pd.DataFrame(
                        [[ticker, 'gain', purchaseDate, possibleSellDate, eEPS,
                          aEPS, purchasePrice, salePriceTarget]],
                        columns=resultColumns
                    ))
                    gains += 1
                    if DEBUG:
                        print(f'Successful sale in {quarter} for ${purchasePrice} -> ${salePriceTarget}')
                    break
                if roi < (lossCutOff * -1):
                    results = results.append(pd.DataFrame(
                        [[ticker, 'loss', purchaseDate, possibleSellDate, eEPS,
                          aEPS, purchasePrice, currentPrice]],
                        columns=resultColumns
                    ))
                    losses += 1
                    if DEBUG:
                        print(f'Drop sale at {round(lossCutOff * 100, 0)}% in {quarter} from ${purchasePrice} -> ${salePriceTarget}')
                    break
                # if last day before earnings, sell
                if track - 1 == weekWindow:
                    diff = currentPrice - purchasePrice
                    loss = diff / purchasePrice
                    print(f'Failsafe sale in {quarter}: '
                          f'realized loss (p: ${purchasePrice}, c: ${currentPrice}) | '
                          f'{round(loss, 2) * 100}%')
    except Exception as e:
        print(e)
    return results


if __name__ == "__main__":
    # PARAMETERS
    DEBUG = False
    gainRoi = .03
    lossCutOff = .08
    weekWindow = 7 * 5
    epsDeltaPerc = .25
    epsDeltaDollar = 1
    # User percentages for EPS delta
    useDeltaPerc = True
    ignoreNegEps = False

    if len(sys.argv) > 2:
        gainRoi = sys.argv[1]
        lossCutOff = sys.argv[2]
        weekWindow = 7 * sys.argv[3]
        epsDeltaPerc = sys.argv[4]

    code = f'g{round(gainRoi*100,0)}-l{round(lossCutOff*100,0)}-w{weekWindow}-e{round(epsDeltaPerc*100,0)}'
    print(f'File code: {code}')

    candidateResults = pd.DataFrame(columns=resultColumns)

    print('Starting Run')

    for row in companies:
        candidateResults = run_company(row, candidateResults)

    candidateResults.to_csv(f'results/file-results-{code}-{timestamp}.csv')

    print('done')

