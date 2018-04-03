from base64 import urlsafe_b64encode
from datetime import datetime
from flask import Flask, request
from googleapiclient import discovery
from os import environ
from signal import SIGHUP, signal
import logging
import traceback
import ujson as json

app = Flask(__name__)
EDGE_TOPICS = dict(json.loads(environ['EDGE_TOPICS']))
NUM_RETRIES = int(environ.get('NUM_RETRIES', 3))
PROJECT_ID = environ['APPLICATION_ID'].split('~').pop()
PUBSUB = discovery.build('pubsub', 'v1')

try:
    with open('version.json') as o:
        version_json = o.read()
except IOError:
    version_json = json.dumps({
        'version': environ.get('CURRENT_VERSION_ID'),
        'application_id': environ.get('APPLICATION_ID'),
    }) + '\n'


class Log(object):
    # default path sends logs to stackdriver logging along with landfill
    fname = environ.get('LOG', '/var/log/app_engine/app.log')
    fp = None

    def hup(self, signum, frame):
        if self.fp is not None:
            self.fp.close()
            self.fp = None

    def write(self, data):
        if self.fp is None:
            self.fp = open(self.fname, 'a')
        self.fp.write(json.dumps(data) + '\n')
        self.fp.flush()


log = Log()
signal(SIGHUP, log.hup)


@app.route('/<path:ping>', methods=['POST'])
def landfill(ping):
    if ping not in EDGE_TOPICS:
        return 'unknown ping type', 404
    topic = EDGE_TOPICS[ping]
    message = {
        'attributes': {
            'agent': request.headers.get('User-Agent'),
            'method': request.method,
            'path': request.path,
            'remote_addr': request.remote_addr,
            'submission_date': datetime.utcnow().strftime(
                '%Y-%m-%d %H:%M:%S'
            ),
        },
        'data': urlsafe_b64encode(request.get_data()).decode('utf8'),
    }
    log.write(message)
    PUBSUB.projects().topics().publish(
        topic='projects/%s/topics/%s' % (PROJECT_ID, topic),
        body={'messages': [message]},
    ).execute(num_retries=NUM_RETRIES)
    return '', 200


@app.route('/__version__')
def version():
    return version_json, 200


@app.route('/__lbheartbeat__')
@app.route('/_ah/health')
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
