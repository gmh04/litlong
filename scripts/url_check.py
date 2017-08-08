#!/usr/bin/env python

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
cur = con.cursor()

fp = os.path.join(data_dir, 'Database - Doc level metadata - Data cleaning.csv')

with open(fp, 'r') as adoc:

    for line in adoc:
        aline = line.split('|')
        doc_id = aline[0]
        if doc_id == 'Primary key':
            continue

        url = aline[6]
        if url[:4] == 'http':
            query = "SELECT count(*) FROM api_document WHERE url = %s and id = %s"
            cur.execute(query, (url, doc_id))
            if cur.fetchone()[0] == 0:
                print 'Update document', doc_id, 'with url', url
                query = 'UPDATE api_document set URL = %s WHERE id = %s'
                cur.execute(query, (url, doc_id))


cur.close()
con.close()
