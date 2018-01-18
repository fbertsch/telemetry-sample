from flask import current_app, Flask, request
from googleapiclient import discovery
from os import environ
import base64

try:
    import ujson as json
except ImportError:
    import json

with open('main.4.bigquery.json') as o:
    main_bq_schema = json.loads(o.read())

bqtype_to_type = {
    'INTEGER': int,
    'FLOAT': float,
    'STRING': str,
    'BOOLEAN': bool,
}


def build_schema(schema_file='main.4.bigquery.json', bq_schema=None):
    if bq_schema is None:
        with open(schema_file) as o:
            bq_schema = json.loads(o.read())
    schema = {}
    for field in bq_schema:
        if field['type'] in bqtype_to_type:
            item = bqtype_to_type[field['type']]
        elif field['type'] == 'RECORD':
            item = build_schema(bq_schema=field['fields'])
        else:
            raise ValueError('unsuppored bigquery type %s' % field['type'])
        if field['mode'] == 'REPEATED':
            item = [item]
        schema[field['name']] = item
    return schema


main_v4_schema = build_schema()


def trim_message(message, schema=main_v4_schema):
    if type(schema) is dict:
        assert type(message) is dict, 'invalid payload'
        return {key: trim_message(message[key], schema[key]) for key in schema if key in message}
    elif type(schema) is list:
        assert type(message) is list, 'invalid payload'
        return [trim_message(element, schema[0]) for element in message]
    else:
        return schema(message)
