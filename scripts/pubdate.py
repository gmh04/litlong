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
        pk = aline[0]
        if pk == 'Primary key':
            continue
        pubdate = aline[5]

        if len(pubdate) != 4:
            continue

        query = "SELECT * FROM api_document WHERE id = {0} AND cast(pubdate as varchar) LIKE $${1}%$$".format(pk, pubdate)

        #print aline[0], aline[5]
        cur.execute(query)
        row = cur.fetchone()
        if row == None:
            print pk, pubdate
            query = 'UPDATE api_document SET pubdate = %s WHERE id = %s'
            cur.execute(query, ('{0}-01-01'.format(pubdate), pk))
            print 'Update document {0} with new pubdate {1}'.format(pk, pubdate)
con.commit()
cur.close()
con.close()
