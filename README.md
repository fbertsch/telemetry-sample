Overview
===

prototype telemetry example for evaluating gcp offerings

Architecture
===

appengine -> pubsub -> appengine -> bigquery

1. appengine receives POST requests from clients containing json telemetry
    payloads, validates the payload schema, deduplicates by document_id, adds
    metadata, and forwards the message to pubsub
2. pubsub writes messages to an auth'd appengine push subscription
3. appengine receives messages from pubsub, transforms them to drop data not
    found in the bigquery schema, and loads them into bigquery via the
    streaming api
4. bigquery stores messages for querying

Deploy
===

create bigquery table

    bq mk telemetry_example
    bq mk --schema schemas/main.4.bigquery.json --time_partitioning_type DAY telemetry_example.main_ping

create pubsub topic

    gcloud pubsub topics create main_ping
    gcloud pubsub subscriptions create main_ping_appengine --topic main_ping --push-endpoint https://telemetry-example-dot-mozilla-data-poc.appspot.com/ah/push-handlers/main_ping

install `libs/`

    # only for OS X
    ! test -e ~/.pydistutils.cfg || mv ~/.pydistutils.cfg{,.bak}
    echo -e '[install]\nprefix=' > ~/.pydistutils.cfg

    pip install -t lib -r requirements.txt

    # only for OS X
    test -e ~/.pydistutils.cfg.bak && mv ~/.pydistutils.cfg{.bak,} || rm ~/.pydistutils.cfg

deploy code:

    gcloud app deploy

