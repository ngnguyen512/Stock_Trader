import json
import requests
from collections import OrderedDict
import math


def convertShares(amount, price):
    return math.trunc(amount / price)


def getStockData(ticker):
    u = url.format(ticker)
    r = requests.get(u)
    tickerData = r.json()
    if list(tickerData.keys())[0] == 'Note':
        print('API Limit')
        exit()
    pricing = tickerData['Time Series (Daily)']

    # we want oldest days first
    return OrderedDict(reversed(list(pricing.items())))


DEBUG = False

apikey = 'SWJCT9RBZM8S1KBE'
url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={{0}}&apikey={apikey}'
tickers = [
    ['DDOG', 76.5],
    ['ESTC', 128.52],
    ['PFE', 48.60],
    ['OKTA', 220],
    ['AMD', 91.36],
    #['NVDA',]
]

# Trading parameters
sellSpread = 0.08
failsafeSpread = 0.08
useFailsafe = True
# Reinvent entire sale, or just initial investment
reInvestGains = True

for ticker in tickers:
    stock = ticker[0]
    # Doesnt matter right now for back testing the strategy
    startPrice = ticker[1]
    # Initial investment will go down if you sell at a loss.
    # will always invest max $1000, so this can only go down
    initialInvestment = 1000
    sharesInvested = convertShares(initialInvestment, startPrice)
    cashProfit = 0

    inverted = getStockData(stock)

    for i, day in enumerate(inverted):
        if i == 0:
            startPrice = float(inverted[day]['1. open'])
            # start date
            if DEBUG:
                print(f'Start trading on: {day}')

        triggerPoint = startPrice + (startPrice * sellSpread)
        failsafePoint = startPrice - (startPrice * failsafeSpread)
        high = float(inverted[day]['2. high'])
        low = float(inverted[day]['3. low'])

        # is a sale triggered?
        if high > triggerPoint:
            if DEBUG:
                print(f'{day} | Selling {stock} at {high}')
            # Capture profit
            cashProfit += (high - startPrice) * sharesInvested
            # reset start price after every sale
            startPrice = high
            # reset shares invested at new price. buy at the high with initialInvestment
            sharesInvested = convertShares(initialInvestment, high)

        # CANT SELL ON LOSS, WHEN DO YOU BUY BACK?
        # if low < failsafePoint and useFailsafe:
        #     if DEBUG:
        #         print(f'{day} | Selling {stock} at {high} (failsafe)')
        #     # Capture loss
        #     cashProfit += (startPrice - low) * sharesInvested
        #     if DEBUG:
        #         print(f'{day} | Loss on {stock} of {math.trunc((startPrice - low) * sharesInvested)}')
        #     # reset start price after every sale
        #     startPrice = low
        #     # get new initialInvestment number because of loss
        #     initialInvestment = low * sharesInvested


    # Total profit for this stock
    print(f'{stock} | ${round(cashProfit, 2)}')

