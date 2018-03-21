from datetime import datetime
from flask import Flask, request
from googleapiclient import discovery
from os import environ
import base64
import logging
import traceback

try:
    import ujson as json
except ImportError:
    import json

app = Flask(__name__)
EDGE_TOPICS = dict(json.loads(environ['EDGE_TOPICS']))
PROJECT_ID = environ['APPLICATION_ID'].split('~').pop()
NUM_RETRIES = int(environ.get('NUM_RETRIES', 3))
PUBSUB = discovery.build('pubsub', 'v1')


@app.route('/<path:ping>', methods=['POST'])
def publish(ping):
    if ping not in EDGE_TOPICS:
        return 'unknown ping type', 404
    topic = EDGE_TOPICS[ping]
    PUBSUB.projects().topics().publish(
        topic='projects/%s/topics/%s' % (PROJECT_ID, topic),
        body={
            'messages': [
                {
                    'attributes': {
                        'agent': request.headers.get('User-Agent'),
                        'method': request.method,
                        'path': request.path,
                        'remote_addr': request.remote_addr,
                        'submission_date': datetime.utcnow().strftime(
                            '%Y-%m-%d %H:%M:%S'
                        ),
                    },
                    'data': base64.urlsafe_b64encode(request.get_data()),
                },
            ],
        },
    ).execute(num_retries=NUM_RETRIES)
    return '', 200


@app.errorhandler(500)
def server_error(e):
    logging.exception('Uncaught Exception')
    # TODO remove traceback from response body
    return traceback.format_exc(), 500
