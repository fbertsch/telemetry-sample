from flask import current_app, Flask, request
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
MESSAGES = []


@app.route('/_ah/push-handlers/messages', methods=['POST'])
def push_messages():
    payload = request.get_json(force=True, silent=True)
    message = json.loads(base64.b64decode(payload['message']['data']))
    MESSAGES.append(payload)
    return '', 204


@app.route('/_ah/push-handlers/message')
def get_messages():
    payload = json.dumps(MESSAGES)
    del MESSAGES[:]
    return payload, 200


@app.errorhandler(500)
def server_error(e):
    logging.exception('Uncaught Exception')
    # TODO remove traceback from response body
    return traceback.format_exc(), 500
