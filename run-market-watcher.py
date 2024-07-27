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
log_group = '/stock-trader/market-watch'
version = 1
eps_delta = 0.2
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


# Call the listingStatus URL and parse the csv returned
# Get all active stock listings on the NYSE and NASDAQ exchanges that we can trade
# Only check existing stocks every 14 days
def get_all_listings(cur, conn):
    response = requests.get(listingStatusUrl)
    csv_data = csv.reader(response.content.decode('utf-8').splitlines())
    headers = next(csv_data)
    data = []
    for row in csv_data:
        data.append(dict(zip(headers, row)))
    # filter on only the exchanges we care about and only stock. OTC stocks have 5 character symbols and Classed stocks have a hyphen in the name
    filtered_data = [row for row in data if (row['exchange'] == 'NYSE' or row['exchange'] == 'NASDAQ') and row['assetType'] == 'Stock' and '-' not in row['symbol'] and len(row['symbol']) < 5]
    # get all existing stock listings in the DB
    sql = f"SELECT * FROM stock_listings WHERE symbol IN ({','.join(['%s']*len(filtered_data))})"
    symbol_list = [d['symbol'] for d in filtered_data]
    cur.execute(sql, symbol_list)
    existing_records = cur.fetchall()
    # Loop through the list of dictionaries and insert each row into the database if needed
    for row in filtered_data:
        # If we have an existing record, only update the listing check date if we are outside the 14 day window
        if row['symbol'] in [r['symbol'] for r in existing_records]:
            existing_record = list(filter(lambda x: x['symbol'] == row['symbol'], existing_records))[0]
            # if the last time we checked the listing was 14 days ago, update it
            # Set the new check date to reset the 14 days. If we are resetting status, reset the Zacks flag so the populate call will check Zacks again.
            if datetime.today() - existing_record['last_listing_status_check_date'] > timedelta(days=14):
                sql = """UPDATE stock_listings
                        SET last_listing_status_check_date = %s, missing_zacks_data = NULL
                        WHERE symbol = %s"""
                values = (datetime.now(), row['symbol'])
                cur.execute(sql, values)
        # If we don't have an existing record, then we have a new listing to track
        else:
            sql = """INSERT INTO stock_listings (symbol, last_listing_status_check_date) 
                 VALUES (%s, %s)"""
            values = (row['symbol'], datetime.now())
            cur.execute(sql, values)
    # Commit the changes to the database
    conn.commit()
    return filtered_data


# Populate our stock listing with earning data from Zacks
# Only update stocks with known Zacks data. If a stock did not have Zacks data, do not check it again until the next listing check.
def populate_earnings_data(cur, conn, listing, seq_token):
    # Get latest check date for each symbol
    cur.execute("""SELECT DISTINCT ON (symbol) symbol, date_checked
                FROM earnings_calendar_history
                ORDER BY symbol, date_checked desc""", ())
    existing_records = cur.fetchall()
    for item in listing:
        symbol = item["symbol"]
        # get DB item for checking if we already checked this stock recently
        existing_record = list(filter(lambda x: x['symbol'] == symbol, existing_records))
        # If we have no record of this, check it.
        # If we have a record and it's at least 2 days old, check it again.
        update_needed = len(existing_record) == 0 or (len(existing_record) > 0 and existing_record[0]['date_checked'] <= datetime.now() - timedelta(days=2))
        if update_needed:
            # If we know that there is no zacks data, skip it
            cur.execute("""SELECT * FROM stock_listings WHERE symbol = %s and missing_zacks_data is TRUE LIMIT 1""", (symbol,))
            # returning a record here, means we to continue and do a recheck
            known_no_zacks_data = cur.fetchone()
            if known_no_zacks_data is not None:
                continue

            # Get number of estimates and last year eps from Zacks
            url = f'https://www.zacks.com/stock/quote/{symbol}/detailed-earning-estimates'
            headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.90 Safari/537.36'}
            try:
                print(f'running Zacks data for population for {symbol}')
                r = requests.get(url, headers=headers)
                html =  BeautifulSoup(r.text, 'html.parser')
                # get the estimate text, and determine if we have to check this one or not.
                try:
                    estimateText = html.find_all(id='detailed_earnings_estimates')[1].find_all('tr')[2].find_all('td')[1].text
                except Exception as e:
                    # Update the database to track which stocks don't have earnings data, so we can stop checking them.
                    set_no_zacks_data(cur, conn, symbol)
                    continue
                if estimateText == 'NA':
                        # Update the database to track which stocks don't have earnings data, so we can stop checking them.
                        set_no_zacks_data(cur, conn, symbol)
                        continue
                
                # check the other data fields and update the database
                numOfEstimates = int(estimateText)
                # if this fails, we don't have an expected earnings date, ignore
                try:
                    expectedEarningsDate = html.find_all(id='detail_estimate')[0].find_all('th')[0].find_all('span')[0].text
                except Exception as e:
                        # Update the database to track which stocks don't have earnings data, so we can stop checking them.
                        set_no_zacks_data(cur, conn, symbol)
                        continue
                
                forecastEps = html.find_all(id='detailed_earnings_estimates')[1].find_all('tr')[1].find_all('td')[1].text
                if forecastEps == 'NA':
                        # Update the database to track which stocks don't have earnings data, so we can stop checking them.
                        set_no_zacks_data(cur, conn, symbol)
                        continue
                
                lastPrice = html.find_all(class_='last_price')[0].text.replace('$', '').replace('USD', '').strip()
                if lastPrice == 'NA':
                        # Update the database to track which stocks don't have earnings data, so we can stop checking them.
                        set_no_zacks_data(cur, conn, symbol)
                        continue
                
                lastYearEPS = html.find_all(id='detailed_earnings_estimates')[1].find_all('tr')[6].find_all('td')[1].text
                lastYearEPS = 0 if lastYearEPS == 'NA' else lastYearEPS
                date = datetime.strptime(expectedEarningsDate, '%m/%d/%y')
                if date.month <= 3:
                    quarter = 1
                elif date.month <= 6:
                    quarter = 2
                elif date.month <= 9:
                    quarter = 3
                else:
                    quarter = 4
                year_quarter = f'{date.year}/{quarter}'

                # get last earnings date
                url = f'https://www.zacks.com/stock/research/{symbol}/earnings-calendar'
                headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.90 Safari/537.36'}
                try:
                    r = requests.get(url, headers=headers)
                    index = r.text.find('earnings_announcements_earnings_table')
                    end = index + 300
                    lastEarningsText = re.search(r'\d{1,2}\/\d{1,2}\/\d{2}', r.text[index:end]).group()
                    lastEarningsDate = datetime.strptime(lastEarningsText, '%m/%d/%y')
                except Exception as e:
                    seq_token = log_event(f'Could not get last earnings date for {symbol}', stream_name, seq_token)
                    continue

                # Add record to the database if we have new data
                print(f'symbol {symbol} has zacks data for update')
                query = """INSERT INTO earnings_calendar_history (symbol, earnings_report_date, num_of_estimates, last_year_eps, year_quarter, last_price, forecast_eps, date_checked, last_earnings_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur.execute(query, (symbol, date, numOfEstimates, lastYearEPS, year_quarter, lastPrice.replace(',', ''), forecastEps, datetime.now(), lastEarningsDate))
                conn.commit()
                # Sleep for random interval to avoid rate limits
                time.sleep(random.randint(2,8))
            except Exception as e:
                conn.rollback()
                continue


def set_no_zacks_data(cur, conn, symbol):
    cur.execute("UPDATE stock_listings SET last_zacks_data_check = %s, missing_zacks_data = TRUE WHERE symbol = %s", (datetime.now(), symbol))
    conn.commit()


def get_stock_price(symbol):
    url = f'https://www.marketwatch.com/investing/stock/{symbol}'
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.90 Safari/537.36'}
    r = requests.get(url, headers=headers)
    html =  BeautifulSoup(r.text, 'html.parser')
    time.sleep(1)
    # get the estimate text, and determine if we have to check this one or not.
    return float(html.find_all(class_='intraday__price')[0].find_all('bg-quote')[0].text)



def check_stock(cur, conn, holding):
    id = holding['id']
    cur.execute('UPDATE trader_sim_log '
                'SET last_checked = %s '
                'WHERE id = %s',
                (datetime.now(), id))
    conn.commit()
    return True


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
    seq_token = None
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    client = boto3.client('logs', region_name='us-west-2')
    stream_name = format_datetime(datetime.now())
    log_stream = client.create_log_stream(
        logGroupName=log_group,
        logStreamName=stream_name
    )

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    today = datetime.now()
    seq_token = log_event(f'Executing Market Watch', stream_name, seq_token)
    seq_token = log_event(f'Version number: {version}', stream_name, seq_token)
    
    # Get all listed companies on the market
    try:
        seq_token = log_event(f'Getting all listings', stream_name, seq_token)
        start = time.time()
        listings = get_all_listings(cur, conn)
        end = time.time()
        seq_token = log_event(f'Getting all listings complete in {round(end - start, 2)} seconds', stream_name, seq_token)
    except Exception as e:
        seq_token = log_event(f'get all listing failed: {e}', stream_name, seq_token)

    try:
        earnings = None
        
        if listings is not None:
            try:
                seq_token = log_event(f'Populate earnings data', stream_name, seq_token)
                start = time.time()
                populate_earnings_data(cur, conn, listings, seq_token)
                end = time.time()
                seq_token = log_event(f'Populate earnings data complete in {round(end - start, 2)} seconds', stream_name, seq_token)
            except Exception as e:
                seq_token = log_event(f'populate earnings data failed: {e}', stream_name, seq_token)
    except Exception as e:
        seq_token = log_event(f'Error: {e}', stream_name, seq_token)

    seq_token = log_event(f'Watch run complete', stream_name, seq_token)

    # Send the log immediately
    streams = client.describe_log_streams(
        logGroupName=log_group,
        logStreamNamePrefix=format_date(datetime.now()),
        orderBy='LogStreamName',
        descending=False,
        limit=50
    )

    message = ''
    for stream_item in streams['logStreams']:
        stream_name = stream_item['logStreamName']

        response = client.get_log_events(
            logGroupName=log_group,
            logStreamName=stream_name,
            limit=10000,
            startFromHead=True
        )

        log_events = response['events']

        for each_event in log_events:
            message += f'{each_event["message"]}\r\n'

    request_url = 'https://api.mailgun.net/v2/{0}/messages'.format(sandbox)
    request = requests.post(request_url, auth=('api', key), data={
        'from': 'postmaster@mailer.crwest.com',
        'to': recipient,
        'subject': 'Market Watch Log',
        'text': message
    })
