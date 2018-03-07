from flask import Flask
from os import environ
import logging
import traceback

app = Flask(__name__)
try:
    with open('version.json') as o:
        version_json = o.read()
except IOError:
    import json
    version_json = json.dumps({
        'version': environ.get('CURRENT_VERSION_ID'),
        'application_id': environ.get('APPLICATION_ID'),
    }) + '\n'


@app.route('/__version__')
def version():
    return version_json, 200


@app.route('/__lbheartbeat__')
def lbheartbeat():
    return '', 200


@app.route('/__heartbeat__')
def heartbeat():
    return 'OK\n', 200


@app.errorhandler(500)
def server_error(e):
    logging.exception('Uncaught Exception')
    # TODO remove traceback from response body
    return traceback.format_exc(), 500
