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

create pubsub topic

    gcloud pubsub topics create main_ping

create pubsub subscription for dataflow

    gcloud pubsub subscriptions create main_ping_dataflow --topic main_ping

create bigquery dataset

    bq mk telemetry_example

create dataflow job to batch from pubsub to bigquery

    cd dataflow
    sbt assembly
    PROJECT="$(gcloud config get-value project)"
    java \
        -jar target/scala-2.12/telemetry-example-dataflow-assembly-0.1.jar \
        --runner=DataFlow \
        --input=pubsub://projects/$PROJECT/subscriptions/main_ping_dataflow \
        --output=bigquery://$PROJECT:telemetry_example.main_ping_v4 \
        --tempLocation=gs://$PROJECT/telemetry_example/ \
        --gcsTempLocation=gs://$PROJECT/telemetry_example/

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
