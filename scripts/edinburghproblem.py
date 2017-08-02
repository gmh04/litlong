#!/usr/bin/env python

# randomly assign mentions to edinburgh locations

import os
import psycopg2
import random

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
            break

con = psycopg2.connect(
   "dbname='{NAME}' user='{USER}' host='{HOST}' port={PORT} password='{PASS}'".format(**db))
cur = con.cursor()


query = "SELECT id FROM api_location WHERE text = 'Edinburgh' AND id > 91";
cur.execute(query)
rows = cur.fetchall()
print rows
ids = [i[0] for i in rows]
print ids
print len(ids)
#exit(0)
query = "SELECT id FROM api_locationmention WHERE location_id IN (SELECT id FROM api_location WHERE text = 'Edinburgh')";
cur.execute(query)
for lmid in cur.fetchall():
    query = "UPDATE api_locationmention SET location_id = %s WHERE id = %s"
    new_loc = random.choice(ids)
    cur.execute(query, (new_loc, lmid[0]))
    print 'update ', lmid[0], ' with ', new_loc

con.commit()

cur.close()
con.close()
