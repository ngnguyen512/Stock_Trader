import requests
import boto3
import datetime

log_group = '/stock-trader/sim'
AWS_REGION = "us-west-2"
client = boto3.client('logs', region_name=AWS_REGION)

key = '5c4f096a0a971f0b09d89ce737bca4b8-c3d1d1eb-71534a86'
sandbox = 'mailer.crwest.com'
recipient = 'cw@crwest.com'


def format_date(date):
    return '%d-%02d-%02d' % (date.year, date.month, date.day)


def send_email(message):
    request_url = 'https://api.mailgun.net/v2/{0}/messages'.format(sandbox)
    request = requests.post(request_url, auth=('api', key), data={
        'from': 'postmaster@mailer.crwest.com',
        'to': recipient,
        'subject': 'Trader Sim Log',
        'text': message
    })


streams = client.describe_log_streams(
    logGroupName=log_group,
    logStreamNamePrefix=format_date(datetime.datetime.now()),
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

send_email(message)
