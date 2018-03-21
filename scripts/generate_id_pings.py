#!/usr/bin/env python

from uuid import uuid4
from sys import argv
from ujson import dumps

rows = 236581135

if len(argv) == 2:
    rows = int(argv[1])

for _ in range(rows):
    print(r'{"data":"{\"id\":\"%s\"}","attributes":{"remote_addr":"2601:1c2:5002:b7c0:7850:a4c9:1683:d1e2","submission_date":"2018-03-21 19:57:58"}}' % uuid4())
