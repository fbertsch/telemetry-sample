#!/usr/bin/env python

import json
from sys import argv
from sys import stderr
from sys import stdin

type_map = {
    'string': 'STRING',
    'boolean': 'BOOLEAN',
    'number': 'FLOAT',
    'integer': 'INTEGER',
}


def resolve_multitype(types, item):
    types = set(types)
    if 'null' in types:
        item['mode'] = 'NULLABLE'
        types.remove('null')
    if 'array' in types:
        item['mode'] = 'REPEATED'
        types.remove('array')
    if len(types) == 1:
        item['type'] = types.pop()
    elif 'object' in types:
        item['type'] = 'object'
    elif 'RECORD' in types:
        item['type'] = 'RECORD'
    elif types - {'integer', 'number', 'boolean'} == {'string'}:
        item['type'] = 'string'
    elif types - {'integer', 'boolean'} == {'number'}:
        item['type'] = 'number'
    elif types - {'boolean'} == {'number'}:
        item['type'] = 'number'
    elif types - {'boolean'} == {'integer'}:
        item['type'] = 'integer'
    elif types - {'INTEGER', 'FLOAT', 'BOOLEAN'} == {'STRING'}:
        item['type'] = 'STRING'
    elif types - {'INTEGER', 'BOOLEAN'} == {'FLOAT'}:
        item['type'] = 'FLOAT'
    elif types - {'BOOLEAN'} == {'FLOAT'}:
        item['type'] = 'FLOAT'
    elif types - {'BOOLEAN'} == {'INTEGER'}:
        item['type'] = 'INTEGER'
    else:
        raise Exception(json.dumps([list(types), item]))


def bq_schema(jschema):
    if type(jschema) is list:
        raise Exception(json.dumps(jschema))
    item = {}
    item['type'] = jschema.get('type')
    if item['type'] is None:
        if 'properties' in jschema:
            item['type'] = 'object'
        elif 'allOf' in jschema:
            item['type'] = 'allOf'
        elif not (set(jschema.keys()) - {'required', 'additionalProperties'}):
            return None
        else:
            return bq_schema({'properties': jschema})
    elif type(item['type']) is list:
        resolve_multitype(item['type'], item)
    if type(item['type']) in [str, unicode] and item['type'] in type_map:
        item['type'] = type_map[item['type']]
    elif item['type'] == 'object':
        item['type'] = 'RECORD'
        item['fields'] = []
        for key, val in jschema.get('properties', {}).items():
            element = bq_schema(val)
            if element is None:
                continue
            element['name'] = key
            if 'mode' not in element:
                element['mode'] = key in jschema.get('required', []) and 'REQUIRED' or 'NULLABLE'
            item['fields'].append(element)
        if not item['fields']:
            return None
    elif item['type'] == 'array':
        item['mode'] = 'REPEATED'
        if 'items' in jschema:
            if type(jschema['items']) is dict:
                item = bq_schema(jschema['items'])
                item['mode'] = 'REPEATED'
            elif type(jschema['items']) is list:
                elements = [bq_schema(element) for element in jschema['items']]
                elements = [element for element in elements if element is not None]
                types = [element['type'] for element in elements]
                resolve_multitype(types, item)
                if item['type'] == 'RECORD':
                    item['fields'] = sum([
                        element.get('fields', []) for element in elements
                    ], [])
        else:
            return None
    elif item['type'] == 'allOf':
        elements = [bq_schema(element) for element in jschema['allOf']]
        elements = [element for element in elements if element is not None]
        if not elements:
            return None
        types = [element['type'] for element in elements]
        resolve_multitype(types, item)
        if item['type'] == 'RECORD':
            item['fields'] = sum([
                element.get('fields', []) for element in elements
            ], [])
    else:
        raise Exception(json.dumps([item,jschema, type(item['type']) in [str, unicode], type(item['type']) in [str, unicode] and item['type'] in type_map]))
    return item

usage = 'usage: jsonschema_to_bigquery.py [<time_partitioning_field> <time_partitioning_type>] < jsonschema.json > bigqueryschema.json\n'

if len(argv) not in [1, 3]:
    stderr.write(usage)
    exit(1)

try:
    jsonschema = json.loads(stdin.read())
except Exception:
    stderr.write(usage)
    exit(1)

schema = bq_schema(jsonschema)

if len(argv) == 3:
    schema['fields'].append({'mode': 'REQUIRED', 'name': argv[1], 'type': argv[2]})

print(json.dumps(schema, indent=2))

