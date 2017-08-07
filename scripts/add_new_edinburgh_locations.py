#!/usr/bin/env python

# https://docs.google.com/spreadsheets/d/11UDJXC5MsBCuyCQurLIZJpdNbPKGZSCC0TyWLM-yAS4/edit#gid=0
# File -> Download as -> ods
# Open
# File -> Save as -> Easter egg locations and coordinates - Sheet.csv | TEXT CSV

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
            break

con = psycopg2.connect(
   "dbname='{NAME}' user='{USER}' host='{HOST}' port={PORT} password='{PASS}'".format(**db))
cur = con.cursor()

#query = "DELETE FROM api_location WHERE id > 40726"
#cur.execute(query)

data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data')
fname = 'Easter egg locations and coordinates.csv'

with open(os.path.join(data_dir, fname), 'r') as doc:

    def get_location_id(lat, lon):
        id = None
        query = "SELECT id FROM api_location WHERE lat = %s AND lon = %s"
        cur.execute(query, (float(lat), float(lon)))
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
        # if desc == 'Napier Merchiston campus':
        #     continue
        # if desc == 'National Gallery':
        #     continue
        #print row
        coords = row[1].split(', ')
        lat = coords[0]
        lon = coords[1]
        loc_id = get_location_id(lat, lon)
        if loc_id:
            print loc_id, desc
            exit(0)
        # else:
        #     print desc
            #exit(0)

        if loc_id == None:
            loc_id = insert_location('Edinburgh', lat, lon)
        print loc_id, desc, lat, lon
        ids.append(loc_id)
    print ids
    #exit(0)

cur.close()
con.close()
