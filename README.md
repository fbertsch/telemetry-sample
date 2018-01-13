to deploy run:

    # only for OS X
    ! test -e ~/.pydistutils.cfg || mv ~/.pydistutils.cfg{,.bak}
    echo -e '[install]\nprefix=' > ~/.pydistutils.cfg

    pip install -t lib -r requirements.txt

    # only for OS X
    test -e ~/.pydistutils.cfg.bak && mv ~/.pydistutils.cfg{.bak,} || rm ~/.pydistutils.cfg

    gcloud app deploy

