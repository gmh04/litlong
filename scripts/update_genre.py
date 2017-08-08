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

query = "DELETE FROM api_document_genre"
cur.execute(query)
query = "DELETE FROM api_genre"
cur.execute(query)
query = "ALTER SEQUENCE api_genre_id_seq RESTART WITH 1"
cur.execute(query)

fp = os.path.join(data_dir, 'Database - Doc level metadata - Data cleaning.csv')

def get_genre_id(genre):
    g_id = None
    query = "SELECT id FROM api_genre WHERE name = $${0}$$".format(genre)
    cur = con.cursor()
    cur.execute(query)
    res = cur.fetchone()
    cur.close()
    if res and len(res) == 1:
        g_id = res[0]
    else:
        g_id = -1;
    return g_id

def do_genre(doc_id, genre):
    success = True

    if len(genre) > 0:
        cur = con.cursor()

        g_id = get_genre_id(genre)
        if g_id == -1:
            query = "INSERT INTO api_genre(name) VALUES ($${0}$$) RETURNING id".format(genre)
            cur.execute(query)
            g_id = cur.fetchone()[0]
            print 'Insert new genre: {0} with id: {1}'.format(genre, g_id)

        query = "INSERT INTO api_document_genre(document_id, genre_id) VALUES ({0}, {1})".format(doc_id, g_id)
        cur.execute(query)
        print "Insert new api_document_genre for {0} => {1} {2}".format(doc_id, g_id, genre)

    return success

with open(fp, 'r') as adoc:

    for line in adoc:
        aline = line.split('|')
        doc_id = aline[0]
        if doc_id == 'Primary key':
            continue

        g1 = aline[8]
        g2 = aline[9]
        g3 = aline[10]

        if not do_genre(doc_id, g1) or not do_genre(doc_id, g2) or not do_genre(doc_id, g3):
            break


con.commit()
cur.close()
con.close()
