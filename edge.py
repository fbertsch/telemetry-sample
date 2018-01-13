from datetime import datetime
from flask import current_app, Flask, request
from googleapiclient import discovery
from os import environ
from ua import ua_parse
from jsonschema import validate
import base64
import logging
import traceback

try:
    import ujson as json
except ImportError:
    import json

app = Flask(__name__)
app.config['PUBSUB_TOPIC'] = environ.get('PUBSUB_TOPIC')
if (
    app.config['PUBSUB_TOPIC'] is not None and
    not app.config['PUBSUB_TOPIC'].startswith('projects/')
):
    app.config['PUBSUB_TOPIC'] = 'projects/%s/topics/%s' % (
        environ['APPLICATION_ID'].split('~').pop(),
        app.config['PUBSUB_TOPIC']
    )
app.config['NUM_RETRIES'] = int(environ.get('NUM_RETRIES', 3))
pubsub = discovery.build('pubsub', 'v1')

with open('main.4.schema.json') as o:
    main_ping_v4_schema = o.read()


@app.route('/', methods=['GET', 'POST'])
@app.route('/<path:path>', methods=['GET', 'POST'])
def publish(path=''):
    date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    payload = request.get_json(force=True, silent=True)
    if type(payload) == list:
        if not payload:
            return '', 204
        for element in payload:
            if type(element) != dict:
                return 'invalid payload, must be array of objects', 400
    elif type(payload) == dict:
        payload = [payload]
    else:
        return 'invalid payload, must be array of objects', 400
    validate(payload, schema)
    meta = {
        'agent': request.headers.get('User-Agent'),
        'method': request.method,
        'path': path,
        'remote_address_chain': request.headers.get('X-Forwarded-For'),
        'time': date,
    }
    for element in payload:
        element.update(meta)
        ua_parse(element, 'agent')
    body = {
        'messages': [
            {
                'data': base64.urlsafe_b64encode(
                    json.dumps(element)
                ),
            }
            for element in payload
        ],
    }
    if current_app.config['PUBSUB_TOPIC'] is not None:
        pubsub.projects().topics().publish(
            topic=current_app.config['PUBSUB_TOPIC'],
            body=body,
        ).execute(num_retries=current_app.config['NUM_RETRIES'])
        return '', 200
    else:
        return json.dumps(body), 200


@app.errorhandler(500)
def server_error(e):
    logging.exception('Uncaught Exception')
    # TODO remove traceback from response body
    return traceback.format_exc(), 500
