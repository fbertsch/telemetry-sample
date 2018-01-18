Deploy
===

create bigquery table

    bq mk --schema main.4.bigquery.json --time_partitioning_type DAY telemetry.main

install `libs/`

    # only for OS X
    ! test -e ~/.pydistutils.cfg || mv ~/.pydistutils.cfg{,.bak}
    echo -e '[install]\nprefix=' > ~/.pydistutils.cfg

    pip install -t lib -r requirements.txt

    # only for OS X
    test -e ~/.pydistutils.cfg.bak && mv ~/.pydistutils.cfg{.bak,} || rm ~/.pydistutils.cfg

deploy code:

    gcloud app deploy


to update requirements, edit `requirements.txt` and repeat previous two steps
