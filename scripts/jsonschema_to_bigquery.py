#!/usr/bin/env python

import json
import unittest
from sys import argv, stderr, stdin


def main():
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


class TestSchemaAtomic(unittest.TestCase):
    """Check that the base case of the schema is being handled properly."""

    def test_atomic(self):
        atomic = {'type': 'integer'}
        expected = {'type': 'INTEGER', 'mode': 'REQUIRED'}

        self.assertEquals(bq_schema(atomic), expected)

    def test_atomic_with_null(self):
        atomic = {'type': ['integer', 'null']}
        expected = {'type': 'INTEGER', 'mode': 'NULLABLE'}

        self.assertEquals(bq_schema(atomic), expected)

    def test_incompatible_atomic_multitype(self):
        """Test overlapping types are treated as json blobs."""

        atomic = {'type': ['boolean', 'integer']}
        expected = {'type': 'STRING', 'mode': 'REQUIRED'}

        self.assertEquals(bq_schema(atomic), expected)

    def test_incompatible_atomic_multitype_with_null(self):
        """Test overlapping types that can be null are nullable json blobs.

        A field is null if any of it's types are null"""

        atomic = {'type': ['boolean', 'integer', 'null']}
        expected = {'type': 'STRING', 'mode': 'NULLABLE'}

        self.assertEquals(bq_schema(atomic), expected)

    def test_incompatible_oneof_atomic_multitype(self):
        incompatible_multitype = {
            "oneOf": [
                {"type": "integer"},
                {"type": "boolean"}
            ]
        }
        expected = {'type': 'STRING', 'mode': 'REQUIRED'}

        self.assertEquals(bq_schema(incompatible_multitype), expected)

    def test_incompatible_oneof_atomic_multitype_with_null(self):
        """Test a oneOf clause and verify that the mode is NULLABLE.

        `null` has a logical-OR like behavior when there are choices of types.
        """

        incompatible_multitype = {
            "oneOf": [
                {"type": ["integer", "null"]},
                {"type": "boolean"}
            ]
        }
        expected = {'type': 'STRING', 'mode': 'NULLABLE'}

        self.assertEquals(bq_schema(incompatible_multitype), expected)


class TestSchemaObject(unittest.TestCase):

    def test_object_with_atomics_is_sorted(self):
        """Test that fields are sorted in a record.

        Sorting makes the output schema deterministic.
        """

        object_atomic = {
            'type': 'object',
            'properties': {
                'field_1': {'type': 'integer'},
                'field_4': {'type': 'number'},
                'field_3': {'type': 'boolean'},
                'field_2': {'type': 'string'},
            }
        }
        expected = {
            'type': 'RECORD',
            'fields': [
                {'name': 'field_1', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'field_2', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'field_3', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                {'name': 'field_4', 'type': 'FLOAT', 'mode': 'NULLABLE'}
            ],
            'mode': 'REQUIRED'
        }

        self.assertEquals(bq_schema(object_atomic), expected)

    def test_object_with_atomics_required(self):
        """Test that required fields have the required mode.

        This changes the mode of the underlying atomic field.
        """
        object_atomic = {
            'type': 'object',
            'properties': {
                'field_1': {'type': 'integer'},
                'field_2': {'type': 'string'},
                'field_3': {'type': 'boolean'},
            },
            'required': ['field_1', 'field_3']
        }
        expected = {
            'type': 'RECORD',
            'fields': [
                {'name': 'field_1', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'field_2', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'field_3', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
            ],
            'mode': 'REQUIRED'
        }

        self.assertEquals(bq_schema(object_atomic), expected)

    def test_object_with_atomics_required_with_null(self):
        """Test the output of a nullable required field.

        The field is casted from nullable to required at the object level.
        Since the underlying field is null, the field is then casted back
        to nullable.
        """

        object_atomic = {
            'type': 'object',
            'properties': {
                'field_1': {'type': ['integer', 'null']},
                'field_2': {'type': 'string'},
                'field_3': {'type': 'boolean'},
            },
            'required': ['field_1', 'field_3']
        }
        expected = {
            'type': 'RECORD',
            'fields': [
                {'name': 'field_1', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'field_2', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'field_3', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
            ],
            'mode': 'REQUIRED'
        }

        self.assertEquals(bq_schema(object_atomic), expected)

    def test_object_with_allof(self):
        """Test that allOf works.

        Why do we do this?
        """

        object_allof = {
            "allOf": [
                {
                    'type': 'object',
                    'properties': {
                        'field_1': {'type': ['integer', 'null']},
                        'field_2': {'type': 'string'},
                        'field_3': {'type': 'boolean'},
                    }
                },
                {'required': ['field_1', 'field_3']}
            ]
        }

        expected = {
            'type': 'RECORD',
            'fields': [
                {'name': 'field_1', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'field_2', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'field_3', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
            ],
            'mode': 'REQUIRED'
        }

        self.assertEquals(bq_schema(object_allof), expected)


    def test_object_with_complex(self):
        object_complex = {
            'type': 'object',
            'properties': {
                'namespace_1': {
                    'type': 'object',
                    'properties': {
                        'field_1': {'type': 'string'},
                        'field_2': {'type': 'integer'}
                    }
                }
            }
        }
        expected = {
            'type': 'RECORD',
            'fields': [
                {
                    'name': 'namespace_1',
                    'type': 'RECORD',
                    'fields': [
                        {'name': 'field_1', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'field_2', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    ],
                    'mode': 'NULLABLE'
                }
            ],
            'mode': 'REQUIRED'
        }

        self.assertEquals(bq_schema(object_complex), expected)


class TestSchemaArray(unittest.TestCase):
    def test_array_with_atomics(self):
        array_atomic = {
            "type": "array",
            "items": {"type": "integer"}
        }
        expected = {'type': 'INTEGER', 'mode': 'REPEATED'}

        self.assertEquals(bq_schema(array_atomic), expected)

    def test_array_with_complex(self):
        array_complex = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "field_1": {"type": "string"},
                    "field_2": {"type": "integer"}
                }
            }
        }
        expected = {
            'mode': 'REPEATED',
            'type': 'RECORD',
            'fields': [
                {'name': 'field_1', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'field_2', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            ]
        }

        self.assertEquals(bq_schema(array_complex), expected)


class TestSchemaMap(unittest.TestCase):
    """Test the behavior of repeated key-value structures.

    This is influenced strongly by the data-structures used in collecting
    metrics. They have different names but common structure.

    This type of output structure can be handled efficiently with the use of
    `UNNEST` and projections.

    An alternative is to dump the entire structure to JSON and use javascript
    UDFs to handle processing.
    """

    def test_map_with_atomics(self):
        map_atomic = {
            "type": "object",
            "additionalProperties": {"type": "integer"}
        }
        expected = {
            'mode': 'REPEATING',
            'type': 'RECORD',
            'fields': [
                {'name': 'key', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'value': 'value', 'type': 'INTEGER', 'mode': 'NULLABLE'}
            ]
        }
        self.assertEquals(bq_schema(map_atomic), expected)

    def test_map_with_complex(self):
        map_complex = {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "field_1": {"type": "string"},
                    "field_2": {"type": "integer"}
                }
            }
        }
        expected = {
            'mode': 'REPEATING',
            'type': 'RECORD',
            'fields': [
                {'name': 'key', 'type': 'STRING', 'mode': 'REQUIRED'},
                {
                    'value': 'value',
                    'type': 'RECORD',
                    'fields': [
                        {'name': 'field_1', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'field_2', 'type': 'INTEGER', 'mode': 'NULLABLE'}
                    ]
                }
            ]
        }
        self.assertEquals(bq_schema(map_complex), expected)

    def test_map_with_pattern_properties(self):
        map_complex = {
            "type": "object",
            "patternProperties": {
                ".+": {"type": "integer"}
            },
            "additionalProperties": False
        }
        expected = {
            'mode': 'REPEATING',
            'type': 'RECORD',
            'fields': [
                {'name': 'key', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'value': 'value', 'type': 'INTEGER', 'mode': 'REQUIRED'}
            ]
        }

        self.assertEquals(bq_schema(map_complex), expected)

    def test_map_with_pattern_and_additional_properties(self):
        map_complex = {
            "type": "object",
            "patternProperties": {
                ".+": {"type": "integer"}
            },
            "additionalProperties": {"type": "integer"}
        }
        expected = {
            'mode': 'REPEATING',
            'type': 'RECORD',
            'fields': [
                {'name': 'key', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'value': 'value', 'type': 'INTEGER', 'mode': 'NULLABLE'}
            ]
        }

        self.assertEquals(bq_schema(map_complex), expected)

    def test_incompatible_map_with_pattern_properties(self):
        incompatible_map = {
            "type": "object",
            "patternProperties": {
                "^S_": {"type": "string"},
                "^I_": {"type": "integer"}
            },
            "additionalProperties": False
        }
        expected = {
            'mode': 'REPEATING',
            'type': 'RECORD',
            'fields': [
                {'name': 'key', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'value': 'value', 'type': 'STRING', 'mode': 'NULLABLE'}
            ]
        }
        self.assertEquals(bq_schema(incompatible_map), expected)

    def test_incompatible_map_with_pattern_and_additional_properties(self):
        incompatible_map = {
            "type": "object",
            "patternProperties": {
                ".+": {"type": "string"}
            },
            "additionalProperties": {"type": "integer"}
        }
        expected = {
            'mode': 'REPEATING',
            'type': 'RECORD',
            'fields': [
                {'name': 'key', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'value': 'value', 'type': 'STRING', 'mode': 'NULLABLE'}
            ]
        }
        self.assertEquals(bq_schema(incompatible_map), expected)


if __name__ == '__main__':
    main()
