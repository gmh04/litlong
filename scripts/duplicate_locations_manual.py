#!/usr/bin/env python

# reassign mentions and delete location based on spreadsheet

# https://docs.google.com/spreadsheets/d/1V66QcurLy38Cu1p4nuCg3C1bMULUR7nSeBE61r8aI7s/edit?ts=59808956#gid=1664470526
# File -> Download as -> ods
# Open
# File -> Save as -> Locations.csv | TEXT CSV

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
cur = con.cursor()

data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data')
fname = 'Locations.csv'

with open(os.path.join(data_dir, fname), 'r') as doc:

    def get_location_name(loc_id):
        name = None
        query = "SELECT text FROM api_location WHERE id = %s"
        cur.execute(query, (loc_id, ))
        row = cur.fetchone()
        if row:
            name = row[0]
        return name

    for line in doc:
        row = line.split('|')
        loc_id = row[0]
        new_loc_id = row[4]
        if new_loc_id:
            loc_name = get_location_name(loc_id)
            if loc_name == None:
                continue

            print 'Move mentions of location {0} ({1}) to {2} ({3})'.format(
                loc_id,
                loc_name,
                new_loc_id,
                get_location_name(new_loc_id)
            )

            query = "SELECT id FROM api_locationmention WHERE location_id = %s"
            cur.execute(query, (loc_id, ))
            mentions = cur.fetchall()
            print 'Location {0} has {1} mention(s)'.format(loc_id, len(mentions))
            for mention in  mentions:
                print 'Update mention {0}, location_id to be {1}'.format(mention[0], new_loc_id)
                query = "UPDATE api_locationmention SET location_id = %s WHERE id  = %s"
                cur.execute(query, (new_loc_id, mention[0]))
            query = "DELETE FROM api_location WHERE id = %s"
            cur.execute(query, (loc_id, ))
            print 'Delete location {0}\n'.format(loc_id)
            print '--------------------------------------------'
            con.commit()
            #exit(0)

cur.close()
con.close()
