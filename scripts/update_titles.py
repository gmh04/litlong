#!/usr/bin/env python

# https://docs.google.com/spreadsheets/d/1kJOqJZd004mIbU3QxEgtUgFyaWmN1KkCCd1BiiUTZJs/edit#gid=0
# update title of each document

import os
import psycopg2

from os.path import expanduser

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

data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data')
fp = os.path.join(data_dir, 'Database - Doc level metadata - Data cleaning.csv')

with open(fp, 'r') as adoc:

    for line in adoc:
        aline = line.split('|')
        doc_id = aline[0]
        title = aline[3]

        if doc_id == 'Primary key':
            continue
        print 'Update document', doc_id, 'with new title', title
        query = "UPDATE api_document SET title = %s WHERE id = %s"
        cur.execute(query, (title, doc_id))

con.commit()
cur.close()
con.close()
