import pytest
import json
import base64


@pytest.fixture
def app():
    import main
    main.app.testing = True
    return main.app.test_client()


def test_lbheartbeat(app):
    r = app.get('/__lbheartbeat__')
    assert r.status_code == 200


def test_heartbeat(app):
    r = app.get('/__heartbeat__')
    assert r.status_code == 200


def test_version(app):
    r = app.get('/__version__')
    assert r.status_code == 200


def test_empty_list_payload(app):
    r = app.get('/', data='[]')
    assert r.status_code == 204


def test_list_payload(app):
    r = app.get('/', data='[{"test":"value"}]')
    assert r.status_code == 200


def test_empty_object_payload(app):
    r = app.get('/', data='{}')
    assert r.status_code == 200


def test_bool_payload(app):
    r = app.get('/', data='true')
    assert r.status_code == 400


def test_string_payload(app):
    r = app.get('/', data='""')
    assert r.status_code == 400


def test_int_payload(app):
    r = app.get('/', data='42')
    assert r.status_code == 400


def test_non_json_payload(app):
    r = app.get('/', data='{"test","value"}')
    assert r.status_code == 400


def test_no_payload(app):
    r = app.get('/')
    assert r.status_code == 400


def test_push_messages(app):
    message_data = json.dumps({"test": "value"}, separators=(',', ':'))
    r = app.post('/_ah/push-handlers/messages', data=json.dumps({
        'message': {
            'data': base64.b64encode(message_data),
            'message_id': '1',
            # 'attributes': {'key': 'value'},
        },
        'subscription': 'projects/myproject/subscriptions/mysubscription',
    }))
    assert r.status_code == 204
    r = app.get('/messages')
    assert r.status_code == 200
    assert r.data == json.dumps([message_data], separators=(',', ':'))
