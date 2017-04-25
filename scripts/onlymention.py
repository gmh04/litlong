#!/usr/bin/env python

from os.path import expanduser

import psycopg2

fp = '{0}/.pgpass'.format(expanduser("~"))
db = {
    'NAME': 'litlong',
}
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


query = "SELECT id FROM api_document"
cur.execute(query)
for i in cur.fetchall():
    #print i[0]
    query = "SELECT m.text, count(*) FROM api_document d, api_locationmention m WHERE d.id = m.document_id AND d.id = {0} GROUP BY m.text;".format(i[0])
    cur.execute(query)
    res =
    if len(res) == 1:
        print '**', res

cur.close()
con.close()
