Overview
===

prototype telemetry example for evaluating gcp offerings

Architecture
===

appengine -> pubsub -> appengine -> pubsub -> (appengine|dataflow) -> bigquery

1. appengine receives POST requests from clients containing json telemetry
    payloads, adds attributes from the http request, and publishes the message
    to pubsub.
2. pubsub writes messages to an auth'd appengine push subscription endpoint.
3. appengine recevies messages from pubsub, validates the payload schema,
    merges attributes over the message, resolves geoip information from the ip,
    scrubs the ip, deduplicates messages by id with memcache.add, and publishes
    the message to pubsub. messages that fail validation are sent to different
    pubsub topics.
4. pubsub writes messages to an auth'd appengine push subscription endpoint or
    waits for dataflow to consume a pull subscription.
5. appengine or dataflow receives messages from pubsub, transforms them to drop
    data not found in the bigquery schema, and loads them into bigquery via the
    streaming api.
4. bigquery stores messages for querying.

Deploy
===

deploy app engine default service

    # app engine requires a default service, so this creates a minimal dockerflow service
    gcloud app deploy appengine/default/app.yaml

download the free GeoLite2 country database

    appengine/scripts/download_geolite2.sh

install `libs/`

    # only for OS X
    ! test -e ~/.pydistutils.cfg || mv ~/.pydistutils.cfg{,.bak}
    echo -e '[install]\nprefix=' > ~/.pydistutils.cfg

    pip install -t appengine/lib -r appengine/requirements.txt

    # only for OS X
    test -e ~/.pydistutils.cfg.bak && mv ~/.pydistutils.cfg{.bak,} || rm ~/.pydistutils.cfg

deploy app engine

    gcloud app deploy appengine/app.yaml

create pubsub topics

    gcloud pubsub topics create main_ping
    gcloud pubsub topics create main_ping_invalid
    gcloud pubsub topics create main_ping_raw
    gcloud pubsub topics create id_ping
    gcloud pubsub topics create id_ping_invalid
    gcloud pubsub topics create id_ping_raw

create pubsub subscriptions for validator

    PROJECT="$(gcloud config get-value project)"
    gcloud pubsub subscriptions create main_ping_validate --topic main_ping_raw --push-endpoint https://telemetry-example-dot-$PROJECT.appspot.com/_ah/push-handlers/validate/main_ping
    gcloud pubsub subscriptions create id_ping_validate --topic id_ping_raw --push-endpoint https://telemetry-example-dot-$PROJECT.appspot.com/_ah/push-handlers/validate/id_ping

create bigquery dataset

    bq mk telemetry_example

App Engine to BigQuery
===

create bigquery tables

    bq mk telemetry_example.main_ping_v4 --schema appengine/schemas/main.4.bigquery.json --time_partitioning_field submission_date --time_partitioning_type DAY
    bq mk telemetry_example.id_ping --schema appengine/schemas/id.bigquery.json --time_partitioning_field submission_date --time_partitioning_type DAY

do this or `Dataflow to BigQuery` not both

    PROJECT="$(gcloud config get-value project)"
    gcloud pubsub subscriptions create main_ping_bigquery --topic main_ping --push-endpoint https://telemetry-example-dot-$PROJECT.appspot.com/_ah/push-handlers/bigquery/main_ping
    gcloud pubsub subscriptions create id_ping_bigquery --topic id_ping --push-endpoint https://telemetry-example-dot-$PROJECT.appspot.com/_ah/push-handlers/bigquery/id_ping

Dataflow to BigQuery
===

do this or `App Engine to BigQuery` not both

create pubsub subscription for dataflow

    gcloud pubsub subscriptions create main_ping_bigquery --topic main_ping
    gcloud pubsub subscriptions create id_ping_bigquery --topic id_ping

create dataflow job to batch from pubsub to bigquery

    cd dataflow
    sbt assembly
    PROJECT="$(gcloud config get-value project)"
    java \
        -jar target/scala-2.12/telemetry-example-dataflow-assembly-0.1.jar \
        --runner=DataFlow \
        --input=pubsub://projects/$PROJECT/subscriptions/main_ping_bigquery \
        --output=bigquery://$PROJECT:telemetry_example.main_ping_v4 \
        --tempLocation=gs://$PROJECT/telemetry_example/main_ping/ \
        --gcsTempLocation=gs://$PROJECT/telemetry_example/main_ping/
    java \
        -jar target/scala-2.12/telemetry-example-dataflow-assembly-0.1.jar \
        --runner=DataFlow \
        --schema=schemas/id.bigquery.json \
        --input=pubsub://projects/$PROJECT/subscriptions/id_ping_bigquery \
        --output=bigquery://$PROJECT:telemetry_example.id_ping \
        --tempLocation=gs://$PROJECT/telemetry_example/main_ping/ \
        --gcsTempLocation=gs://$PROJECT/telemetry_example/main_ping/

Validate
===

Send pings to appengine

    PROJECT="$(gcloud config get-value project)"
    curl -d @appengine/test/1.json https://telemetry-example-dot-$PROJECT.appspot.com/main_ping -i
    curl -d @appengine/test/2.json https://telemetry-example-dot-$PROJECT.appspot.com/main_ping -i
    curl -d @appengine/test/3.json https://telemetry-example-dot-$PROJECT.appspot.com/main_ping -i
    # send them again to see that they are all determined to be duplicates
    curl -d @appengine/test/1.json https://telemetry-example-dot-$PROJECT.appspot.com/main_ping -i
    curl -d @appengine/test/2.json https://telemetry-example-dot-$PROJECT.appspot.com/main_ping -i
    curl -d @appengine/test/3.json https://telemetry-example-dot-$PROJECT.appspot.com/main_ping -i

Check that pings arrived in bigquery

    bq query --nouse_cache --nouse_legacy_sql 'select submission_date, id from telemetry_example.main_ping_v4 order by submission_date'
