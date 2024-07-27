import numpy as np
import pandas as pd
from datetime import timedelta, datetime
import time

def getDate(date):
    return datetime.strptime(date, '%Y-%m-%d')


resultColumns = ['symbol', 'event', 'purchaseDate', 'purchasePrice', 'estimatedEPSThisYear', 'actualEPSLastYear', 'sellDate', 'sellPrice']
candidateResults = pd.DataFrame(columns=resultColumns)


def getClosestOpenDate(stockPricing, targetDate):
    start = targetDate - timedelta(days=3)
    end = targetDate + timedelta(days=3)
    validDates = stockPricing[(stockPricing['date'] > start) & (stockPricing['date'] < end)]
    return validDates.head()


summary = 'dataset_summary.csv'
summaryData = pd.read_csv(summary)

stocksWithEarningsData = summaryData[summaryData['total_earnings'] > 0]

earnings = 'stocks_latest/earnings_latest.csv'
earningsData = pd.read_csv(earnings)
earningsData['date'] = pd.to_datetime(earningsData['date'])

stocks = 'stocks_latest/stock_prices_latest.csv'
stockData = pd.read_csv(stocks, dtype={
                    'symbol': 'str',
                    'date': 'str',
                    'open': 'float32',
                    'high': 'float32',
                    'low': 'float32',
                    'close': 'float32',
                    'close_adjusted': 'float32',
                    'volume': 'float32',
                    'split_coefficient': 'float32',
                 })
stockData['date'] = pd.to_datetime(stockData['date'])

timestamp = time.time()

print('Start Run')

DEBUG = False
roiSwitch = .03
weekWindow = 7 * 4
epsDeltaPerc = .25
ignoreNegEps = False

gains = 0
losses = 0

try:
    for index, stockSummary in stocksWithEarningsData.iterrows():
        # Get earnings frame for ticker
        ticker = stockSummary['symbol']
        # print(ticker)

        # get the start date of the earnings data
        fromDate = stockSummary['earnings_from_date']
        # find all acutal earnings data from above that start date
        validEarnings = earningsData[
            (earningsData['date'] > fromDate) & (earningsData['symbol'] == ticker) & (earningsData['eps'] > 0)]
        # This ticket now has valid earnings dates.
        # Run the compare
        for i, earning in validEarnings.iterrows():
            quarter = earning['date']
            lastYearActualEPS = earning['eps']

            # Look forward one year to get YoY earnings estimate
            nextYearQuarter = quarter + timedelta(days=365)
            # day might not match up exactly
            start = nextYearQuarter - timedelta(days=2)
            end = nextYearQuarter + timedelta(days=2)

            nextYearQuarterData = validEarnings[(validEarnings['date'] > start) & (validEarnings['date'] < end)]
            if len(nextYearQuarterData) == 0:
                if DEBUG:
                    print(f'no future quarter to track for {ticker}: {quarter}')
                continue
            # Get EPS numbers for compare
            # Filter next year data on values existing
            nextYearEstimatedEPSValues = nextYearQuarterData[(nextYearQuarterData['eps_est'] > 0)]
            nextYearEstimatedEPS = nextYearEstimatedEPSValues.head()['eps_est']
            thisTicketStockData = stockData[stockData['symbol'] == ticker]

            # Check EPS delta
            if nextYearEstimatedEPS.isnull().values.any():
                print(f'Estimated EPS data missing {ticker}')
                continue
            try:
                epsDiff = float(nextYearEstimatedEPS) - float(lastYearActualEPS)
                epsDelta = epsDiff / float(lastYearActualEPS)
                if epsDelta < epsDeltaPerc:
                    if DEBUG:
                        print(f'EPS delta too small, not tracking for quarter {quarter}')
                    continue
            except Exception:
                print(f'failed to convert float from: {nextYearEstimatedEPS}')
                continue

            priceTargetDate = quarter - timedelta(days=weekWindow)
            try:
                openDate = getClosestOpenDate(thisTicketStockData, priceTargetDate)
            except Exception:
                continue

            # We have a potential buy event!
            # Convert to date
            openDate['date'] = pd.to_datetime(openDate['date'])
            purchaseDate = openDate['date'].dt.date
            purchasePrice = openDate['open'].to_numpy()[0]
            print(f'Buy {ticker} at {purchasePrice}')

            # Start the EPS loop
            # Now loop the next 4 weeks of days to find a 2% gain for sale
            for i in range(weekWindow):
                track = i + 1
                nextDate = priceTargetDate + timedelta(days=track)
                priceDate = thisTicketStockData[thisTicketStockData['date'] == nextDate]

                try:
                    currentPrice = float(priceDate['high'].to_numpy()[0])
                except Exception:
                    continue
                # is this a 2% gain?
                diff = currentPrice - purchasePrice
                roi = diff / purchasePrice
                if roi > roiSwitch:
                    gains += 1
                    break
                if roi < -.02:
                    losses += 1
                    break
except Exception as e:
    print(e)

print(f'Loss ratio for {epsDeltaPerc * 100}% EPS delta and {weekWindow} day window and {round(roiSwitch * 100, 0)}% target')
print(f'Gains: {gains}. Losses: {losses}. | {round(losses/(gains+losses) * 100, 2)}%')
