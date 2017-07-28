#!/usr/bin/env python

# sets documents in deletions.csv to inactive

import os
import psycopg2

from os.path import expanduser

data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data')

db = {
    'NAME': 'litlong',
}

fp = '{0}/.pgpass'.format(expanduser("~"))
with open(fp, 'r') as pgpass:
    for line in pgpass:
        aline = line.split(':')
        if aline[2] == db['NAME']:
            db['HOST'] = aline[0]
            db['PORT'] = aline[1]
            db['USER'] = aline[3]
            db['PASS'] = aline[4].strip()
con = psycopg2.connect(
    "dbname='{NAME}' user='{USER}' host='{HOST}' port={PORT} password='{PASS}'".format(**db));

fp = os.path.join(data_dir, 'deletions.csv')
with open(fp, 'r') as adoc:
    cur = con.cursor()

    for line in adoc:
        aline = line.split('|')

        pk = aline[0]

        if pk == 'Primary key':
            continue

        query = "UPDATE api_document SET active = FALSE WHERE id = {0}".format(pk)
        cur.execute(query)

    con.commit()
    cur.close()
