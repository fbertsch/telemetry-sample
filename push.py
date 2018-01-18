from flask import current_app, Flask, request
from googleapiclient import discovery
from os import environ
from trim_message import build_schema
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
app.config['NUM_RETRIES'] = int(environ.get('NUM_RETRIES', 3))
bigquery = discovery.build('bigquery', 'v2')

bq_schema_files = dict(json.loads(environ['BIGQUERY_SCHEMA_FILES']))
bq_schemas = {
    table: build_schema(path)
    for table, path in bq_schema_files.items()
}


@app.route('/_ah/push-handlers/<path:table>', methods=['POST'])
def push_messages(table='main'):
    if table not in bq_schemas:
        return 'unknown table', 404
    payload = request.get_json(force=True, silent=True)
    message = json.loads(base64.b64decode(payload['message']['data']))
    if 'time' in message:
        date = message['time'][:4] + message['time'][5:7] + message['time'][8:10]
        message = trim_message(message, bq_schemas[table])
        response = bigquery.tabledata().insertAll(
            projectId=current_app.config['PROJECT_ID'],
            datasetId=current_app.config['BIGQUERY_DATASET'],
            tableId=table + '$' + date,
            body={'rows': [{'json': message}]},
        ).execute(num_retries=current_app.config['NUM_RETRIES'])
    # TODO log invalid field errors in response
    return '', 204


@app.errorhandler(500)
def server_error(e):
    logging.exception('Uncaught Exception')
    # TODO remove traceback from response body
    return traceback.format_exc(), 500
