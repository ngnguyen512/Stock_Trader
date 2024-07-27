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

DEBUG = False
apikey = ''
apikey2 = ''
apikey3 = ''
simpleMovingAverageUrl = f'https://www.alphavantage.co/query?function=SMA&interval=weekly&time_period=52&series_type=open&symbol={{0}}&apikey={{1}}'
earningsCalendarUrl = f'https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&horizon=3month&apikey={{1}}'
stockPriceUrl = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&outputsize=full&symbol={{0}}&apikey={{1}}'
api = 1


def getMovingAverage(ticker):
    if api == 1:
        u = simpleMovingAverageUrl.format(ticker, apikey)
    elif api == 2:
        u = simpleMovingAverageUrl.format(ticker, apikey2)
    else:
        u = simpleMovingAverageUrl.format(ticker, apikey3)

    r = requests.get(u)
    tickerData = r.json()
    if 'quarterlyEarnings' not in tickerData.keys():
        if list(tickerData.keys())[0] == 'Note' or list(tickerData.keys())[0] == 'message':
            print('API Limit')
            time.sleep(65)
            return getCompanyEarnings(ticker)
        return [], False

    earnings = tickerData['quarterlyEarnings']
    # cache the earnings, since the data doesnt change.
    with open(filename, "w") as outfile:
        json.dump(earnings, outfile, indent=4)
    # we want oldest days first
    return list(reversed(earnings)), False


companies = [

]

try:
    for k, company in enumerate(companies):
        if k == 0:  # Header row
            continue
        ticker = company[0]
        # did we process this already?
        if skipFlag:
            if ticker == skipCut:
                skipFlag = False
            continue
        print(f'Tracking: {ticker} | {company[1]} on a min {epsDeltaPerc * 100}% EPS delta and {weekWindow} day window')

        earnings, loaded = getCompanyEarnings(ticker)
        if len(earnings) == 0:
            # We have no data from API
            continue
        # only get the pricing once per company, API limits
        allPricing = None

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
            if allPricing is None:
                temp = getStockPrice(ticker)
                if len(temp) == 0:
                    continue
                allPricing = temp
            # Run the compare
            try:
                eEPS = float(nextYearEstimatedEPS)
            except ValueError:
                continue

            aEPS = float(lastYearQuarterActualEPS)
            if float(aEPS) == 0.0:
                if DEBUG:
                    print(f'Actual EPS is zero, cannot get delta {quarter}')
                continue
            epsDiff = float(eEPS) - float(aEPS)
            epsDelta = epsDiff / float(aEPS)
            # check if the delta is because the numbers are really small
            #  or abs(epsDiff) < .5
            # Do I care?
            if epsDelta < epsDeltaPerc:
                if DEBUG:
                    print(f'EPS delta too small, not tracking for quarter {quarter}')
                continue

            # get price 4 weeks before the report date
            priceTargetDate = quarter - timedelta(days=weekWindow)
            try:
                openDate, purchaseDate = getClosestOpenDate(allPricing, priceTargetDate)
            except Exception:
                continue

            # We are purchasing the stock!!
            purchasePrice = float(openDate['1. open'])

            # Now loop the next 4 weeks of days to find a 2% gain for sale
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
                priceDelta = (purchasePrice * roiSwitch)
                # if the delta between the buy and sell date is less than
                # the resulting dollar amount of the roi switch being less
                # than one cent, sell at the next cent marker.
                # ex. stock price $0.49, roi delta 2%, difference $0.0098 (less than one cent)
                if priceDelta < 0.01:
                    priceDelta = 0.01

                salePriceTarget = round(purchasePrice + priceDelta, 2)
                if roi > roiSwitch:
                    candidateResults = candidateResults.append(pd.DataFrame(
                        [[ticker, 'gain', purchaseDate, possibleSellDate, eEPS,
                          aEPS, purchasePrice, salePriceTarget]],
                        columns=resultColumns
                    ))
                    print(f'Successful sale in {quarter} for ${purchasePrice} -> ${salePriceTarget}')
                    break
                if roi < (roiSwitch * -1):
                    candidateResults = candidateResults.append(pd.DataFrame(
                        [[ticker, 'loss', purchaseDate, possibleSellDate, eEPS,
                          aEPS, purchasePrice, salePriceTarget]],
                        columns=resultColumns
                    ))
                    print(f'Drop sale at 2% in {quarter} from ${purchasePrice} -> ${round(purchasePrice - priceDelta, 2)}')
                    break
                # if last day before earnings, sell
                if track - 1 == weekWindow:
                    diff = currentPrice - purchasePrice
                    loss = diff / purchasePrice
                    print(f'Failsafe sale in {quarter}: '
                          f'realized loss (p: ${purchasePrice}, c: ${currentPrice}) | '
                          f'{round(loss, 2) * 100}%')

        # write checkpoint
        if k % 10 == 0:
            candidateResults.to_csv(f'results/results-{code}-{timestamp}.csv')
            print("write checkpoint")


        time.sleep(20)

finally:
    candidateResults.to_csv(f'results/results-{code}-{timestamp}.csv')




