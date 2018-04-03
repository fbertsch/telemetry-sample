#!/bin/python

from os import environ, kill, remove, rename
from os.path import exists, getsize
from signal import SIGHUP, SIGINT, SIGTERM, signal
from subprocess import run, Popen
from time import sleep
from socket import gethostname
from datetime import datetime, timedelta

app_id = environ.get('APPLICATION_ID', '')
flask_proc = None
dest = environ.get('DEST', 'gs://%s/' % app_id) + gethostname() + '-%s.gz'
log = environ.get('LOG', '/var/log/app_engine/app.log')
log_max_size = int(environ.get('LOG_MAX_SIZE', 1024))
rotate_seconds = timedelta(seconds=int(environ.get('ROTATE_SECONDS', 60)))
next_rotate = datetime.now() + rotate_seconds
uploaded = 0


def size(path):
    try:
        return getsize(path)
    except OSError:
        return 0


def rotate(exiting=False):
    global uploaded
    global next_rotate
    if exists(log) and size(log) > 0:
        rename(log, '/var/log/app_engine/landfill')
        if not exiting and flask_proc is not None:
            flask_proc.send_signal(SIGHUP)
            sleep(0.1)
        run(['gzip', '/var/log/app_engine/landfill'])
        run(['gsutil', 'cp', '/var/log/app_engine/landfill.gz', dest % uploaded])
        remove('/var/log/app_engine/landfill.gz')
        uploaded += 1
        next_rotate = datetime.now() + rotate_seconds


def maybe_rotate():
    if next_rotate < datetime.now() or size(log) > log_max_size:
        rotate()


def flask_running():
    if flask_proc is not None:
        try:
            kill(flask_proc.pid, 0)
            return True
        except OSError:
            pass
    return False


def term(signum, frame):
    print('terminating')
    if flask_proc is not None:
        try:
            flask_proc.terminate()
            flask_proc.wait()
        except:
            pass
    rotate(exiting=True)
    exit(signum!=SIGTERM)


signal(SIGTERM, term)
signal(SIGINT, term)

flask_proc = Popen(['flask', 'run', '--host=0.0.0.0', '--port=8080'])

while True:
    sleep(5)
    if not flask_running():
        term(None, None)
    maybe_rotate()
