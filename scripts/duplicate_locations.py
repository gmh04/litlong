#!/usr/bin/env python

# 1) find all locations that are duplicated by text, lat lon
# 2) take first location to keep
# 3) assign all mentions to the locations to be kept
# 4) delete obsolete location

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

query = "SELECT count(text), text, lat, lon FROM api_location GROUP BY text, lat, lon"
cur = con.cursor()
cur.execute(query)
for i in cur.fetchall():
    if int(i[0]) > 1:
        query = "SELECT id FROM api_location WHERE text = %s AND lat = %s AND lon = %s"
        cur.execute(query, (i[1], i[2], i[3]))

        ids = cur.fetchall()
        print "Location '{0}' has {1} instances. keep {2}".format(i[1], i[0], ids[0][0])
        keep_id = ids[0][0]

        for j in ids[1:]:
            loc_id = j[0]
            query = "SELECT id FROM api_locationmention WHERE location_id = %s"
            cur.execute(query, (loc_id, ))
            mentions = cur.fetchall()
            print 'Location {0} has {1} mention(s)'.format(loc_id, len(mentions))
            for mention in  mentions:
                print 'Update mention {0} location_id to be {1}'.format(mention[0], keep_id)
                query = "UPDATE api_locationmention SET location_id = %s WHERE id  = %s"
                cur.execute(query, (keep_id, mention[0]))
            query = "DELETE FROM api_location WHERE id = %s"
            cur.execute(query, (loc_id, ))
            print 'Delete location {0}\n'.format(loc_id)

        #exit(0)

con.commit()
cur.close()
con.close()
