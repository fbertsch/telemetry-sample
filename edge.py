from datetime import datetime
from flask import current_app, Flask, request
from googleapiclient import discovery
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from os import environ
from probables.cuckoo.cuckoo import CuckooFilter
from ua import ua_parse
import base64
import logging
import traceback

try:
    import ujson as json
except ImportError:
    import json

app = Flask(__name__)
app.config['PROJECT_ID'] = environ['APPLICATION_ID'].split('~').pop()
app.config['NUM_RETRIES'] = int(environ.get('NUM_RETRIES', 3))
pubsub = discovery.build('pubsub', 'v1')

cf = CuckooFilter(capacity=1000)

schema_files = dict(json.loads(environ['EDGE_SCHEMA_FILES']))
schemas = {}
for topic, path in schema_files.items():
    with open(path) as o:
        schemas[topic] = json.loads(o.read())


@app.route('/<path:topic>', methods=['POST'])
def publish(topic=''):
    if topic not in schemas:
        return 'unknown ping type', 404
    date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    payload = request.get_json(force=True, silent=True)
    try:
        validate(payload, schemas[topic])
    except ValidationError as e:
        return 'invalid payload: %s' % e, 400
    if cf.check(payload['id']):
        return 'duplicate\n', 200
    else:
        cf.add(payload['id'])
    meta = {
        'agent': request.headers.get('User-Agent'),
        'method': request.method,
        'path': path,
        'remote_address_chain': request.headers.get('X-Forwarded-For'),
        'time': date,
    }
    payload.update(meta)
    ua_parse(payload, 'agent')
    body = {
        'messages': [
            {
                'data': base64.urlsafe_b64encode(
                    json.dumps(payload)
                ),
            }
        ],
    }
    pubsub.projects().topics().publish(
        topic='projects/%s/topics/%s' % (current_app.config['PROJECT_ID'], topic),
        body=body,
    ).execute(num_retries=current_app.config['NUM_RETRIES'])
    return '', 200


@app.errorhandler(500)
def server_error(e):
    logging.exception('Uncaught Exception')
    # TODO remove traceback from response body
    return traceback.format_exc(), 500
