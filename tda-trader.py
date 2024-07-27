import json
import requests
from datetime import datetime, timedelta
import time
import psycopg2
import psycopg2.extras
import math
import boto3
import logging
import sys
import random
import uuid
from td.client import TDClient
from td.orders import Order, OrderLeg

conn = psycopg2.connect("host=45.77.120.179 dbname=other user=other password=oDyK7fGjwqY6D")
u = 'https://api.apify.com/v2/actor-tasks/coryrwest~nasdaq-earnings-calendar/run-sync-get-dataset-items?token=W4GwNpKcdLvgCQkWwSw7T6FBA'

stockPriceUrl = 'https://realstonks.p.rapidapi.com/'
tdClientId = 'LNIU6W7CH8GZCUIS9ZZRBLYGIIGCH2EW'
tdRedirect = 'https://invest.crwest.com/oauth'
tdCredentialsFile = 'ameritrade-credentials.json'

logger = logging.getLogger(__name__)
log_group = '/stock-trader/etrade-log'
version = 1
days_window = 49
eps_delta = 0.2
investment_per = 400
gain_cutoff = .035

def format_date(date):
    return '%d-%02d-%02d' % (date.year, date.month, date.day)


def format_datetime(date):
    return '%d-%02d-%02d-%02d-%02d-%02d' % \
           (date.year, date.month, date.day, date.hour, date.minute, date.second)


def get_fresh_earnings(date):
    if date.weekday() > 4:
        date = date - timedelta(days=1)
        if date.weekday() > 4:
            date = date - timedelta(days=1)
    headers = {
        'Content-Type': "application/json",
    }
    r = requests.post(u, f'{{"customData": {{"date":"{format_date(date)}"}}}}', headers=headers, timeout=180)
    earningsData = r.json()
    return earningsData[0]["earningsData"]


def get_stock_price(symbol):
    headers = {
        'x-rapidapi-host': "realstonks.p.rapidapi.com",
        'x-rapidapi-key': "OyDIsD5Lg4mshbltktN99f8PQ1DIp1z51iUjsnEDqmqsaFpWad"
    }

    r = requests.request("GET", f'{stockPriceUrl}{symbol}', headers=headers)
    data = r.json()
    time.sleep(1)
    return data['price']


def buy_order(td_client, account_id, symbol, shares):
    order = Order()
    order.order_session('NORMAL')
    order.order_duration('DAY')
    order.order_type('MARKET')
    order.order_strategy_type("SINGLE")

    orderLeg = OrderLeg()
    orderLeg.order_leg_instruction("BUY")
    orderLeg.order_leg_quantity(shares)
    orderLeg.order_leg_quantity_type('SHARES')
    orderLeg.order_leg_asset(asset_type="EQUITY",symbol=symbol)

    order.add_order_leg(order_leg=orderLeg)

    resp = td_client.place_order(account_id, order)
    return resp


def sell_limit_order(td_client, account_id, symbol, shares, limit_price):
    order = Order()
    order.order_session('NORMAL')
    order.order_duration('GOOD_TILL_CANCEL')
    order.order_type('LIMIT')
    order.order_strategy_type("SINGLE")

    orderLeg = OrderLeg()
    orderLeg.order_leg_instruction("SELL")
    orderLeg.order_leg_quantity_type('ALL_SHARES')
    orderLeg.order_leg_price(limit_price)
    orderLeg.order_leg_asset(asset_type="EQUITY",symbol=symbol)

    order.add_order_leg(order_leg=orderLeg)

    resp = td_client.place_order(account_id, order)
    return resp


def log_stock_buy(cur, conn, price, target_price, earnings_date):
    cur.execute('INSERT INTO etrader_trade_log ('
                'purchase_date, symbol, purchase_price,'
                'target_sell_price, last_checked, shares,'
                'total_cost, earnings_report_date) VALUES ('
                '%s, %s, %s,'
                '%s, %s, %s,'
                '%s, %s, %s)',
                (datetime.now(), symbol, round(price, 2),
                 target_price, datetime.now(), shares,
                 round(price * shares, 2), earnings_date))
    conn.commit()


def sell_stock(cur, conn, holding, current_price):
    id = holding['id']
    cost = holding['total_cost']
    shares = holding['shares']
    ret = round((shares * current_price) - cost, 2)
    cur.execute('UPDATE trader_sim_log '
                'SET sell_price = %s,'
                'sell_date = %s,'
                'total_return = %s '
                'WHERE id = %s',
                (round(current_price, 2), datetime.now(), ret, id))
    conn.commit()
    return ret


def log_event(message, stream_name, seq_token):
    log_event = {
        'logGroupName': log_group,
        'logStreamName': stream_name,
        'logEvents': [
            {
                'timestamp': int(round(time.time() * 1000)),
                'message': message
            },
        ],
    }
    if seq_token:
        log_event['sequenceToken'] = seq_token
    response = client.put_log_events(**log_event)
    print(f'LOGGED: {message}')
    return response['nextSequenceToken']

if __name__ == "__main__":
    morning_run = sys.argv[1] if len(sys.argv) > 1 else False
    # If this is not a local run, we cannot continue if there is a missing token. Human intervention required.
    local_run = sys.argv[2] if len(sys.argv) > 2 else False
    seq_token = None
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    client = boto3.client('logs', region_name='us-west-2')
    stream_name = format_datetime(datetime.now())
    log_stream = client.create_log_stream(
        logGroupName=log_group,
        logStreamName=stream_name
    )
    today = datetime.now()
    check_date = today + timedelta(days=days_window)
    accountid = '255167369'

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    seq_token = log_event(f'Gathering API information', stream_name, seq_token)

    TDSession = TDClient(
        client_id=tdClientId,
        redirect_uri=tdRedirect,
        credentials_path=tdCredentialsFile
    )

    # Login to the session
    TDSession.login()

    if morning_run:
        seq_token = log_event(f'This is the morning run, make our buys for the day.', stream_name, seq_token)
        # version log
        seq_token = log_event(f'etrader version number: {version}', stream_name, seq_token)

        try:
            earnings = None
            # try:
            #     seq_token = log_event(f'Getting fresh earnings data for {format_date(check_date)}, may take up to 4 minutes.', stream_name, seq_token)
            #     earnings = get_fresh_earnings(check_date)
            # except Exception as e:
            #     seq_token = log_event(f'Earnings fetch failed: {e}', stream_name, seq_token)
            earnings = [
                {'lastYearEPS': '$0.54', 'ticker': 'AZN', 'estimates': 4, 'forecastEPS': '$0.75',
                            'lastYearReportDate': '11/12/2021', 'fiscalQuarterEnd': 'Sep/2022', 'date': '2022-11-11'}
            ]

            if earnings is not None:
                try:
                    # Do we want to make any purchases today?
                    for earning in earnings:
                        symbol = earning['ticker']
                        # Ignore negatives
                        if '(' in earning['lastYearEPS'] or '(' in earning['forecastEPS']:
                            seq_token = log_event(f'Ignoring negative EPS for {symbol}', stream_name, seq_token)
                            continue
                        lastYearEPS = float(earning['lastYearEPS'].replace('$', ''))
                        forecastEPS = float(earning['forecastEPS'].replace('$', ''))
                        # Get the EPS delta
                        epsDiff = forecastEPS - lastYearEPS
                        epsDelta = epsDiff / lastYearEPS
                        seq_token = log_event(f'Testing EPS delta. (F: {forecastEPS} | A: {lastYearEPS})', stream_name,
                                              seq_token)
                        if epsDelta >= eps_delta:
                            seq_token = log_event(f'Event Found for {symbol}', stream_name, seq_token)
                            expectedReportDate = \
                                datetime.strptime(earning['lastYearReportDate'], '%m/%d/%Y') \
                                + timedelta(days=365)
                            # Make sure the report date for this year is not a weekend
                            # Go backwards to make sure we have data
                            if expectedReportDate.weekday() > 4:
                                expectedReportDate = expectedReportDate - timedelta(days=1)
                                if expectedReportDate.weekday() > 4:
                                    expectedReportDate = expectedReportDate - timedelta(days=1)
                            # Make our buy
                            current_price = get_stock_price(symbol)
                            shares = math.floor(investment_per / current_price)
                            if shares == 0:
                                seq_token = log_event(f'{symbol} share was above max investment (${investment_per}), purchasing one share at {current_price}', stream_name, seq_token)
                                shares = 1
                                continue
                            # Buy the stock
                            seq_token = log_event(f'Attempting stock purchase for {symbol} at ${current_price}', stream_name, seq_token)
                            target_price = current_price * (current_price * gain_cutoff)
                            purchased = False
                            try:
                                buy = buy_order(TDSession, accountid, symbol, shares)
                                if buy['status_code'] == 200:
                                    # set the limit order as well
                                    sell_limit = sell_limit_order(TDSession, accountid, symbol, shares, target_price)
                                    purchased = True
                                else:
                                    seq_token = log_event(f'Buy order for {symbol} ({buy["order_id"]}) failed with status code {buy["status_code"]}', stream_name,
                                                          seq_token)
                            except Exception as e:
                                seq_token = log_event(f'Error during stock purchase for {symbol}. {e}', stream_name, seq_token)
                            seq_token = log_event(f'Purchase made for {shares} shares at ${current_price} for a cost of {current_price * shares}.', stream_name, seq_token)

                            if purchased:
                                seq_token = log_event(f'Logging purchase', stream_name, seq_token)
                                log_stock_buy(cur, conn, current_price, target_price, expectedReportDate)
                except Exception as e:
                    seq_token = log_event(f'Symbol Error: {e}', stream_name, seq_token)
        except Exception as e:
            seq_token = log_event(f'Application Error: {e}', stream_name, seq_token)

    #
    # # Do we want to sell anything today?
    # holdings = get_pending_trades(cur)
    # current_value = 0
    # for holding in holdings:
    #     try:
    #         target_price = holding['target_sell_price']
    #         symbol = holding['symbol']
    #         earnings_report = holding['earnings_report_date']
    #         shares = holding['shares']
    #         cost = holding['total_cost']
    #         try:
    #             current_price = get_stock_price(symbol)
    #         except Exception as e:
    #             # Wait 10 seconds on holding get fail to make sure that it's actually a failure.
    #             time.sleep(10)
    #             current_price = get_stock_price(symbol)
    #         check_stock(cur, conn, holding)
    #         # have we hit target?
    #         seq_token = log_event(f'Processing current holding for {symbol}', stream_name, seq_token)
    #         if current_price >= target_price:
    #             # sell the stock
    #             seq_token = log_event(f'Target hit. Selling {shares} of {symbol} at ${current_price}', stream_name, seq_token)
    #             roi = sell_stock(cur, conn, holding, current_price)
    #             current_balance += (roi + cost)
    #             seq_token = log_event(f'Current balance: {current_balance}', stream_name, seq_token)
    #             continue
    #         # is it the day before earnings?
    #         # make sure we sell on a Friday if earnings falls on Monday
    #         cutoff_date = earnings_report - timedelta(days=1)
    #         if cutoff_date.weekday() > 5:
    #             cutoff_date = cutoff_date - timedelta(days=1)
    #         if datetime.now() >= cutoff_date:
    #             # failsafe sale
    #             roi = sell_stock(cur, conn, holding, current_price)
    #             current_balance += (roi + cost)
    #             seq_token = log_event(f'Failsafe sale for {symbol}. Selling {shares} shares at ${current_price}', stream_name, seq_token)
    #             continue
    #         # Update current value of holdings we aren't selling
    #         current_value += round(current_price * shares, 2)
    #     except Exception as e:
    #         seq_token = log_event(f'Holding Error: {e}', stream_name, seq_token)
    #
    # seq_token = log_event(f'Current portfolio value: {round(current_value, 2)}', stream_name, seq_token)
    #
    # # recalculate current balance
    # total_ret = get_total_return(cur)
    # total_invested = get_total_invested(cur)
    # current_balance = round(meta['opening_balance'] + total_ret - total_invested, 2)
    #
    # seq_token = log_event(f'Current re-calculated balance: {current_balance}', stream_name, seq_token)
    #
    # cur.execute('INSERT INTO trader_sim_meta (opening_balance, current_balance,'
    #             'investment_per_event, last_updated, last_compounding,'
    #             'gain_cutoff, day_window, eps_delta, scenario) VALUES ('
    #             '%s, %s,'
    #             '%s, %s, %s,'
    #             '%s, %s, %s, %s)',
    #             (meta['opening_balance'], current_balance,
    #              investment_per, datetime.now(), datetime.now(),
    #              gain_cutoff, day_window, eps_delta, meta['scenario']))
    # conn.commit()
    seq_token = log_event(f'Run complete', stream_name, seq_token)
