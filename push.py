from flask import current_app, Flask, request
from googleapiclient import discovery
from os import environ
from trim_message import trim_message
import base64
import logging
import traceback

try:
    import ujson as json
except ImportError:
    import json

app = Flask(__name__)
app.config['BIGQUERY_DATASET'] = environ['BIGQUERY_DATASET']
app.config['PROJECT_ID'] = environ['APPLICATION_ID'].split('~').pop()
app.config['BIGQUERY_TABLE'] = environ['BIGQUERY_TABLE']
app.config['NUM_RETRIES'] = int(environ.get('NUM_RETRIES', 3))
bigquery = discovery.build('bigquery', 'v2')

with open('main.4.bigquery.json') as o:
    main_bq_schema = json.loads(o.read())


@app.route('/_ah/push-handlers/messages', methods=['POST'])
def push_messages():
    payload = request.get_json(force=True, silent=True)
    message = json.loads(base64.b64decode(payload['message']['data']))
    if 'time' in message:
        date = message['time'][:4] + message['time'][5:7] + message['time'][8:10]
        message = trim_message(message)
        response = bigquery.tabledata().insertAll(
            projectId=current_app.config['PROJECT_ID'],
            datasetId=current_app.config['BIGQUERY_DATASET'],
            tableId=current_app.config['BIGQUERY_TABLE'] + '$' + date,
            body={'rows': [{'json': message}]},
        ).execute(num_retries=current_app.config['NUM_RETRIES'])
    # TODO log invalid field errors in response
    return '', 204


@app.errorhandler(500)
def server_error(e):
    logging.exception('Uncaught Exception')
    # TODO remove traceback from response body
    return traceback.format_exc(), 500
