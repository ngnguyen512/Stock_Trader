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
conn = psycopg2.connect("host=45.77.120.179 dbname=other user=other password=F3X3q7h8irUK")
u = 'https://api.apify.com/v2/actor-tasks/coryrwest~nasdaq-earnings-calendar/run-sync-get-dataset-items?token=W4GwNpKcdLvgCQkWwSw7T6FBA'

alphaVantageApi = 'ORL7L6P47R2DYAS3'
stockPriceUrl = 'https://realstonks.p.rapidapi.com/'
avEarningsCalendar = f'https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&horizon=3month&apikey={alphaVantageApi}'
listingStatusUrl = f'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={alphaVantageApi}'

logger = logging.getLogger(__name__)
log_group = '/stock-trader/sim'
version = 43
cohort_start = '12/18/23'
price_cutoff = 25
# emails
key = '5c4f096a0a971f0b09d89ce737bca4b8-c3d1d1eb-71534a86'
sandbox = 'mailer.crwest.com'
recipient = 'cw@crwest.com'

def parse_date(dateString):
    return datetime.strptime(dateString, '%Y-%m-%d')


def format_date(date):
    return '%d-%02d-%02d' % (date.year, date.month, date.day)


def format_datetime(date):
    return '%d-%02d-%02d-%02d-%02d-%02d' % \
           (date.year, date.month, date.day, date.hour, date.minute, date.second)


def get_all_listings(cur, conn):
    sql = """SELECT * FROM stock_listings WHERE missing_zacks_data is FALSE"""
    values = (datetime.now(), row['symbol'])
    cur.execute(sql, values)
    data = cur.fetchall()
    return data


def get_stock_price(symbol):
    url = f'https://www.cnbc.com/quotes/{symbol}'
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.90 Safari/537.36'}
    r = requests.get(url, headers=headers)
    html =  BeautifulSoup(r.text, 'html.parser')
    tit = r.text
    if 'the page you were looking for cannot be found.' in tit:
      raise Exception('Symbol does not exist on CNBC')
    time.sleep(1)
    # get the estimate text, and determine if we have to check this one or not.
    return float(html.find_all(class_='QuoteStrip-lastPrice')[0].text.replace(',', ''))


def get_pending_trades(cur):
    cur.execute('SELECT * FROM trader_sim_log WHERE sell_date is null',
                ())
    return cur.fetchall()


def has_pending_trade(cur, symbol):
    cur.execute('SELECT * FROM trader_sim_log WHERE symbol = %s and sell_date is null',
                (symbol, ))
    return len(cur.fetchall()) > 0


def has_past_trade(cur, symbol):
    cur.execute('SELECT * FROM trader_sim_log WHERE symbol = %s and sell_date is not null and earnings_report_date > now()',
                (symbol, ))
    return len(cur.fetchall()) > 0


def get_total_return(cur):
    cur.execute('select sum(total_return) from trader_sim_log', ())
    total = cur.fetchone()
    return 0 if total["sum"] is None else total["sum"]


def get_total_invested(cur):
    cur.execute('select sum(total_cost) from trader_sim_log where sell_date is null', ())
    total_invested = cur.fetchone()
    return 0 if total_invested["sum"] is None else total_invested["sum"]


def buy_stock(cur, conn, symbol, price, shares, gain_cutoff, earnings_date):
    target = round(price + (price * gain_cutoff), 2)
    preempt_target = round(price + (price * .02), 2)
    next_estimated_date = earnings_date + timedelta(days=90)
    cur.execute('INSERT INTO trader_sim_log ('
                'purchase_date, symbol, purchase_price,'
                'target_sell_price, last_checked, shares,'
                'total_cost, earnings_report_date, next_estimated_earnings_date, prempt_target_sell_price) VALUES ('
                '%s, %s, %s,'
                '%s, %s, %s,'
                '%s, %s, %s, %s)',
                (datetime.now(), symbol, round(price, 2),
                 target, datetime.now(), shares,
                 round(price * shares, 2), earnings_date, next_estimated_date, preempt_target))
    conn.commit()
    return True


def check_stock(cur, conn, holding):
    id = holding['id']
    cur.execute('UPDATE trader_sim_log '
                'SET last_checked = %s '
                'WHERE id = %s',
                (datetime.now(), id))
    conn.commit()
    return True


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


def send_email(message, file):
    request_url = 'https://api.mailgun.net/v2/{0}/messages'.format(sandbox)
    return requests.post(request_url, auth=('api', key), data={
        'from': 'postmaster@mailer.crwest.com',
        'to': 'westropp@gmail.com',
        'subject': 'Daily Earnings Deltas',
        'text': message,
        },
        files=[("attachment", file)],
    )


if __name__ == "__main__":
    run_earnings = sys.argv[1] if len(sys.argv) > 1 else False
    seq_token = None
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    client = boto3.client('logs', region_name='us-west-2')
    stream_name = format_datetime(datetime.now())
    log_stream = client.create_log_stream(
        logGroupName='/stock-trader/sim',
        logStreamName=stream_name
    )

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    today = datetime.now()

    # 4339
    cur.execute('SELECT * FROM trader_sim_meta order by last_updated desc limit 1',
                ())
    meta = cur.fetchone()
    investment_per = meta['investment_per_event']
    gain_cutoff = meta['gain_cutoff']
    # unused for actual trading
    day_window = meta['day_window']
    eps_delta = meta['eps_delta']
    check_date = today + timedelta(days=day_window)
    earnings = None
    current_balance = meta['current_balance']
    seq_token = log_event(f'Executing check for version {version}, cohort start: {cohort_start}', stream_name, seq_token)
    seq_token = log_event(f'Current Balance: {current_balance}', stream_name, seq_token)

    if run_earnings:
        try:
            # Get all the possible stocks that match our criteria
            cur.execute(f'''SELECT symbol, earnings_report_date, last_earnings_date, last_year_eps, forecast_eps, round(CAST((forecast_eps - last_year_eps) / last_year_eps as numeric), 2) as delta
                FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date_checked DESC) AS row_num
                FROM earnings_calendar_history
                WHERE num_of_estimates > 5 AND last_price > 25 AND earnings_report_date > current_date AND forecast_eps > 0 and last_year_eps > 0 AND ((forecast_eps - last_year_eps) / last_year_eps) > {eps_delta}
                ) subquery
                WHERE row_num = 1
                ORDER BY earnings_report_date DESC''', ())
            earnings = cur.fetchall()
        except Exception as e:
            seq_token = log_event(f'Earnings fetch failed: {e}', stream_name, seq_token)

        # get the blacklist
        cur.execute(f'''SELECT symbol FROM trader_blacklist''', ())
        blacklist = cur.fetchall()

        # call database to get first checked date for each earning and set blacklist
        for earn in earnings:
            try:
                cur.execute(f"select date_checked from earnings_calendar_history where symbol = '{earn['symbol']}' and earnings_report_date = '{earn['earnings_report_date']}' order by date_checked limit 1")
                noticeDate = cur.fetchone()
                earn['firstNoticed'] = format_date(noticeDate["date_checked"])
                # set blacklist
                if earn['symbol'] in [bl['symbol'] for bl in blacklist]:
                    earn['blacklisted'] = True
                else:
                    earn['blacklisted'] = False
            except Exception as e:
                seq_token = log_event(f'Could not retrieve first noticed date for {earn["symbol"]} {earn["earnings_report_date"]}', stream_name, seq_token)

        with open('earnings.csv', "w", newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            writer.writerow(earnings[0].keys())
            for earn in earnings:
                writer.writerow(earn.values())
        # reorder the csv
        with open('earnings.csv', 'r') as infile, open('ordered-earnings.csv', 'a', newline='') as outfile:
            # output dict needs a list for new column ordering
            fieldnames = ['symbol', 'firstNoticed', 'earnings_report_date', 'last_earnings_date', 'last_year_eps', 'forecast_eps', 'delta', 'blacklisted']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            # reorder the header first
            writer.writeheader()
            for row in csv.DictReader(infile):
                # writes the reordered rows to the new file
                writer.writerow(row)
        # send the email
        seq_token = log_event(f'Sending earnings delta email', stream_name, seq_token)
        response = send_email(f'Daily earnings deltas for {today}', open('ordered-earnings.csv'))
        seq_token = log_event(f'Sent email {response}', stream_name, seq_token)
        os.remove('ordered-earnings.csv')

        if earnings is not None:
            seq_token = log_event(f'{len(earnings)} Total Events Found', stream_name, seq_token)
            try:
                # Do we want to make any purchases today?
                # Any item in this list will match the delta requirement and min price because the SQL query checks that
                for earning in earnings:
                    symbol = earning['symbol']
                    # ---------------- SINGLE STOCK TEST ----------------------
                    # if symbol != 'TDG':
                    #     continue
                    # ---------------- SINGLE STOCK TEST ----------------------
                    # check if symbol is in blacklist, ignore if so
                    if earning['blacklisted']:
                        seq_token = log_event(f'Blacklisted {symbol}, ignoring', stream_name, seq_token)
                        continue
                    if current_balance > investment_per:
                        seq_token = log_event(f'Event Found for {symbol}', stream_name, seq_token)
                        expectedReportDate = earning['earnings_report_date']
                        # Make sure the report date for this year is not a weekend
                        # Go backwards to make sure we have data
                        if expectedReportDate.weekday() > 4:
                            expectedReportDate = expectedReportDate - timedelta(days=1)
                            if expectedReportDate.weekday() > 4:
                                expectedReportDate = expectedReportDate - timedelta(days=1)
                        # don't buy within 10 days of earnings
                        tenDaysBefore = expectedReportDate - timedelta(days=10)
                        tenDaysAfter = expectedReportDate + timedelta(days=10)
                        if today.date() >= tenDaysBefore.date():
                            seq_token = log_event(f'Event is within the earnings blackout window {symbol}. Earnings date: {format_date(expectedReportDate)}. Blackout Start: {format_date(tenDaysBefore)}', stream_name, seq_token)
                            continue
                        # Make our buy                        
                        try:
                            current_price = get_stock_price(symbol)
                        except Exception as e:
                            seq_token = log_event(f'Get Stock Price Error: {symbol}, skipping', stream_name, seq_token)
                            seq_token = log_event(f'{e}', stream_name, seq_token)
                            continue
                        # Recheck price cutoff on latest price
                        if current_price < price_cutoff:
                            seq_token = log_event(f'Price under ${price_cutoff}, ignoring {symbol} at ${current_price}',
                                                    stream_name,
                                                    seq_token)
                            continue
                        shares = math.floor(investment_per / current_price)
                        if shares == 0 and current_price < (investment_per * 2):
                            seq_token = log_event(f'{symbol} share was above max investment (${investment_per}), priced at {current_price}', stream_name, seq_token)
                            shares = 1
                        # if we have a pending trade, ignore this stock
                        pending = has_pending_trade(cur, symbol)
                        if pending:
                            seq_token = log_event(f'Ignoring for pending trade, {symbol}', stream_name, seq_token)
                            continue
                        # if we purchased this stock during this earnings period already, ignore
                        past_purchase = has_past_trade(cur, symbol)
                        if past_purchase:
                            seq_token = log_event(f'This stock ({symbol}) has a already been purchased this period, ignore',
                                                    stream_name,
                                                    seq_token)
                            continue
                        # Buy the stock
                        seq_token = log_event(f'Attempting stock purchase for {symbol} at ${current_price}', stream_name, seq_token)
                        cost = round(current_price * shares, 2)
                        if current_balance < cost:
                            seq_token = log_event(f'Cannot make purchase for {symbol}, not enough money. Cost: ${cost}, Balance: ${current_balance}',
                                                    seq_token)
                            continue
                        purchased = buy_stock(cur, conn, symbol, current_price, shares, gain_cutoff, expectedReportDate)
                        current_balance = current_balance - cost
                        seq_token = log_event(f'Purchase made for {shares} shares at ${current_price} for a cost of ${cost}. Remaining balance: ${current_balance}', stream_name, seq_token)
                    elif current_balance < investment_per:
                        seq_token = log_event(f'Could not make purchase for {symbol}. Out of money. Remaining balance: ${current_balance}', stream_name, seq_token)
            except Exception as e:
                seq_token = log_event(f'Symbol Error. Not all symbol events processed. {e}', stream_name, seq_token)


    # Do we want to sell anything today?
    holdings = get_pending_trades(cur)
    current_value = 0
    for holding in holdings:
        try:
            target_price = holding['target_sell_price']
            prempt_target_sell_price = holding['prempt_target_sell_price']
            symbol = holding['symbol']
            earnings_report = holding['earnings_report_date']
            shares = holding['shares']
            purchase_date = holding['purchase_date']
            cost = holding['total_cost']
            try:
                try:
                    current_price = get_stock_price(symbol)
                except Exception as e:
                    if 'Symbol does not exist on MarketWatch' in str(e):
                        seq_token = log_event(f'Get Stock Price Error: No data on MarketWatch for {symbol}', stream_name, seq_token)
                        continue
                    # Wait 10 seconds on holding get fail to make sure that it's actually a failure.
                    time.sleep(10)
                    current_price = get_stock_price(symbol)
            except Exception as e:
                seq_token = log_event(f'Get Stock Price Error: {e} | {holding}', stream_name, seq_token)
                continue
            try:
                check_stock(cur, conn, holding)
            except Exception as e:
                seq_token = log_event(f'Check Stock Error: {e} | {holding}', stream_name, seq_token)
            # have we hit target?
            #seq_token = log_event(f'Processing current holding for {symbol}', stream_name, seq_token)
            # if we hit the preempt target price early, sell it!
            if current_price >= prempt_target_sell_price and today.date() <= (purchase_date.date() + timedelta(days=2)):
                # sell the stock
                seq_token = log_event(f'Preempt target hit early. Selling {shares} of {symbol} at ${current_price}', stream_name, seq_token)
                roi = sell_stock(cur, conn, holding, current_price)
                current_balance += (roi + cost)
                seq_token = log_event(f'Current balance: {current_balance}', stream_name, seq_token)
                continue
            if current_price >= target_price:
                # sell the stock
                seq_token = log_event(f'Target hit. Selling {shares} of {symbol} at ${current_price}', stream_name, seq_token)
                roi = sell_stock(cur, conn, holding, current_price)
                current_balance += (roi + cost)
                seq_token = log_event(f'Current balance: {current_balance}', stream_name, seq_token)
                continue
            # is it time to sell failsafe?
            # make sure we sell on a Friday if failsafe falls on a weekend
            cutoff_date = purchase_date + timedelta(days=5)
            if cutoff_date.weekday() == 5:
                cutoff_date = cutoff_date - timedelta(days=1)
            if cutoff_date.weekday() == 6:
                cutoff_date = cutoff_date - timedelta(days=2)
            if datetime.now() >= cutoff_date:
                # failsafe sale
                roi = sell_stock(cur, conn, holding, current_price)
                current_balance += (roi + cost)
                seq_token = log_event(f'Failsafe sale for {symbol}. Selling {shares} shares at ${current_price}, with cost of ${cost}, for return of ${roi}', stream_name, seq_token)
                continue
            # Update current value of holdings we aren't selling
            current_value += round(current_price * shares, 2)
        except Exception as e:
            seq_token = log_event(f'Holding Error: {e}', stream_name, seq_token)

    seq_token = log_event(f'Current portfolio value: {round(current_value, 2)}', stream_name, seq_token)

    # recalculate current balance
    total_ret = get_total_return(cur)
    total_invested = get_total_invested(cur)
    current_balance = round(meta['opening_balance'] + total_ret - total_invested, 2)

    seq_token = log_event(f'Current re-calculated balance: ${current_balance}, total return: ${total_ret}', stream_name, seq_token)

    cur.execute('INSERT INTO trader_sim_meta (opening_balance, current_balance,'
                'investment_per_event, last_updated, last_compounding,'
                'gain_cutoff, day_window, eps_delta, scenario) VALUES ('
                '%s, %s,'
                '%s, %s, %s,'
                '%s, %s, %s, %s)',
                (meta['opening_balance'], current_balance,
                 investment_per, datetime.now(), datetime.now(),
                 gain_cutoff, day_window, eps_delta, meta['scenario']))
    conn.commit()
    seq_token = log_event(f'Run complete', stream_name, seq_token)
