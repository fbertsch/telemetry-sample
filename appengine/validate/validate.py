from flask import Flask, request
from geoip2.database import MODE_MEMORY
from geoip2.database import Reader as Geoip2Reader
from googleapiclient import discovery
from google.appengine.api import memcache
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from os import environ
from ua import ua_parse
from uuid import UUID
from base64 import b64decode, urlsafe_b64encode
import logging
import traceback

try:
    import ujson as json
except ImportError:
    import json

app = Flask(__name__)
GEOIP2_DB = Geoip2Reader(environ['GEOIP2_DATABASE'], mode=MODE_MEMORY)
TOPICS_IF_INVALID = dict(json.loads(environ.get('TOPICS_IF_INVALID', '{}')))
SCHEMAS = {}
for topic, path in dict(json.loads(environ['VALIDATE_SCHEMAS'])).items():
    with open(path) as o:
        SCHEMAS[topic] = json.loads(o.read())
PROJECT_ID = environ['APPLICATION_ID'].split('~').pop()
NUM_RETRIES = int(environ.get('NUM_RETRIES', 3))
PUBSUB = discovery.build('pubsub', 'v1')


@app.route('/_ah/push-handlers/<path:topic>', methods=['POST'])
def handle(topic):
    if topic not in SCHEMAS:
        return 'unknown ping type', 404
    message = request.get_json(force=True, silent=True)['message']
    data = b64decode(message['data'])
    # validate
    try:
        ping = json.loads(data)
        validate(ping, SCHEMAS[topic])
    except ValidationError as e:
        # add error to message
        message['attributes']['error'] = 'invalid schema: %s' % e
        # send to rejects topic
        return publish(message, TOPICS_IF_INVALID.get(topic))
    except ValueError as e:
        # add error to message
        message['attributes']['error'] = 'invalid json: %s' % e
        # send to rejects topic
        return publish(message, TOPICS_IF_INVALID.get(topic))
    # deduplicate
    try:
        uuid = UUID(ping['id']).bytes
        assert(memcache.add(key=uuid, value="", time=86400)) # expires after one day
    except AssertionError:
        # add error to message
        message['attributes']['error'] = 'duplicate id: %s' % ping['id']
        # send to rejects topic
        return publish(message, TOPICS_IF_INVALID.get(topic))
    except ValueError:
        # add error to message
        message['attributes']['error'] = 'invalid id: %s' % e
        # send to rejects topic
        return publish(message, TOPICS_IF_INVALID.get(topic))
    # merge attributes
    ping.update(message['attributes'])
    # parse user agent
    ua_parse(ping, 'agent')
    # geoip
    if 'remote_addr' in ping:
        ip = ping.pop('remote_addr')
        try:
            ping['country'] = GEOIP2_DB.country(ip).country.name
        except:
            pass
    return publish({'data': urlsafe_b64encode(json.dumps(ping))}, topic)


def publish(message, topic):
    if topic is not None:
        PUBSUB.projects().topics().publish(
            topic='projects/%s/topics/%s' % (PROJECT_ID, topic),
            body={'messages': [message]},
        ).execute(num_retries=NUM_RETRIES)
        return '', 204
    else:
        message['data'] = b64decode(message['data'])
        print(json.dumps(message))
        return '', 200


@app.errorhandler(500)
def server_error(e):
    logging.exception('Uncaught Exception')
    # TODO remove traceback from response body
    return traceback.format_exc(), 500
