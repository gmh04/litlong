#!/usr/bin/env python

from os.path import expanduser

import os
import psycopg2
import requests
import urllib

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
            break
con = psycopg2.connect(
   "dbname='{NAME}' user='{USER}' host='{HOST}' port={PORT} password='{PASS}'".format(**db))

dup=[]

query = "SELECT count(text), lat, lon  FROM api_location GROUP BY lat, lon"
cur = con.cursor()
cur.execute(query)
for i in cur.fetchall():
    if int(i[0]) > 1:
        query = "SELECT text FROM api_location WHERE lat = %s AND lon = %s"
        cur.execute(query, (i[1], i[2]))

        s = set([])
        for i in cur.fetchall():
            s.add(i[0])
        if len(s) > 1:
            print ", ".join(s)

print len(dup)
