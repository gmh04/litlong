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

fp = os.path.join(data_dir, 'Database - Doc level metadata - Data cleaning2.csv')

with open(fp, 'r') as adoc:

    for line in adoc:
        aline = line.split('|')
        doc_id = aline[0]
        if doc_id == 'Primary key':
            continue

        g1 = aline[9]
        g2 = aline[10]
        g3 = aline[11]
        genres = [g1]
        if g2:
            genres.append(g2)
        if g3:
            genres.append(g2)

        query = "SELECT count(*) FROM api_document_genre WHERE document_id = %s"
        cur.execute(query, (doc_id,))

        count = cur.fetchone()[0]
        if len(genres) != count:
            print doc_id, count, '{0}|{1}|{2}'.format(g1, g2, g3)

cur.close()
con.close()
