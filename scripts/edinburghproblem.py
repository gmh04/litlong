#!/usr/bin/env python

import os
import psycopg2
import random

from os.path import expanduser
from shutil import copyfile

# https://docs.google.com/spreadsheets/d/11UDJXC5MsBCuyCQurLIZJpdNbPKGZSCC0TyWLM-yAS4/edit#gid=0
# File -> Download as -> ods
# Open
# File -> Save as -> Easter egg locations and coordinates - Sheet1.csv | TEXT CSV

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

data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data')
fname = 'Easter egg locations and coordinates - Sheet1.csv'

with open(os.path.join(data_dir, fname), 'r') as doc:

    def get_location_id(text):
        id = None
        query = "SELECT id FROM api_location WHERE text = %s"
        cur.execute(query, (text,))
        row = cur.fetchone()
        if row != None:
            id = row[0]
        return id

    def insert_location(text, lat, lon):
        query = "INSERT INTO api_location(text, lat, lon, geom) values (%(text)s, %(lat)s, %(lon)s, ST_GeomFromEWKT('SRID=4326;POINT(%(lon)s %(lat)s)')) RETURNING id"

        cur.execute(query, {'text': text, 'lon': float(lon), 'lat': float(lat)})
        id = cur.fetchone()[0]
        con.commit()
        return id

    ids = []
    for line in doc:
        row = line.split('|')
        desc = row[0]
        if desc == 'Location description ':
            continue

        loc_id = get_location_id(desc)
        coords = row[1].split(', ')
        lat = coords[0]
        lon = coords[1]
        if loc_id == None:
            loc_id = insert_location(desc, lat, lon)
        print loc_id, desc, lat, lon
        ids.append(loc_id)
    print ids
    #print()

    query = "SELECT id FROM api_locationmention WHERE location_id IN (SELECT id FROM api_location WHERE text = 'Edinburgh')";
    cur.execute(query)
    for lmid in cur.fetchall():
        query = "UPDATE api_locationmentiom SET location_id = %s WHERE id = %s"
        new_loc = random.choice(ids)
        #cur.execute(query, (new_loc, lmid[0]))
        print 'update ', lmid[0], ' with ', new_loc



cur.close()
con.close()
