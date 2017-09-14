#!/usr/bin/env python
# -*- coding: utf-8 -*-

# https://docs.google.com/spreadsheets/d/1kJOqJZd004mIbU3QxEgtUgFyaWmN1KkCCd1BiiUTZJs/edit#gid=0
# File -> Download as -> ods
# Open
# File -> Save as -> Database - Doc level metadata - Data cleaning2.csv | TEXT CSV

import os
import psycopg2
import sys

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
    "dbname='{NAME}' user='{USER}' host='{HOST}' port={PORT} password='{PASS}'".format(**db));
cur = con.cursor()

UNKOWN = ['Anon.', 'A Yankee', 'Eudora', 'Evangeline', 'Porte', 'Yankee']

def document_genre_exists(doc_id, genre_id):
    query = "SELECT count(*) FROM api_document_genre WHERE genre_id = %s AND document_id = %s"
    cur.execute(query, (genre_id, doc_id))
    return cur.fetchone()[0] > 0

def document_author_exists(doc_id, author_id):
    query = "SELECT count(*) FROM api_document_author WHERE author_id = %s AND document_id = %s"
    cur.execute(query, (author_id, doc_id))
    return cur.fetchone()[0] > 0

def get_author_id(first_name, surname):
    a_id = None
    query = "SELECT id FROM api_author WHERE forenames = $${0}$$ AND surname = $${1}$$".format(first_name, surname)
    cur.execute(query)
    res = cur.fetchone()
    if res and len(res) == 1:
        a_id = res[0]
    else:
        a_id = -1;
    return a_id

def get_genre_id(genre):
    g_id = None
    query = "SELECT id FROM api_genre WHERE name = %s"
    cur.execute(query, (genre,))
    res = cur.fetchone()
    if res and len(res) == 1:
        g_id = res[0]
    else:
        g_id = -1;
    return g_id

def get_publisher_id(publisher):
    p_id = None
    query = "SELECT id FROM api_publisher WHERE name = %s"
    cur.execute(query, (publisher,))
    res = cur.fetchone()
    if res and len(res) == 1:
        p_id = res[0]
    else:
        p_id = -1;
    return p_id

def do_author(doc_id, name, gender):
    if len(name) > 0:
        names = name.split(',')
        if len(names) != 2:
            if names[0] in UNKOWN:
                names.append('')
                gender = 'unknown'
            else:
                print 'Name is not correct:  ', a1
                exit(0)

        first_name = names[1]
        surname = names[0]

        author_id = get_author_id(first_name, surname)
        if author_id < 0:
            author_id = insert_author(first_name, surname, gender)
        else:
            print "author '{0}' already exists".format(name)

        if not document_author_exists(doc_id, author_id):
            insert_document_author(doc_id, author_id)
            print "Insert new api_document_author for {0} => {1}".format(name, doc_id)
        else:
            print 'api_document_author entry exists'

def do_genre(doc_id, genre):
    genre_id = get_genre_id(genre)
    if genre_id < 0:
        genre_id = insert_genre(genre)
    else:
        print "genre '{}' already exists".format(genre)

    if not document_genre_exists(doc_id, genre_id):
        insert_document_genre(doc_id, genre_id)
        print "Insert new api_document_genre for {0} => {1}".format(genre, doc_id)
    else:
        print 'api_document_genre entry exists'

def do_pubdate(doc_id, pubdate):
    if len(pubdate) != 4 or pubdate == '????':
        print '** Invalid date', pubdate
        return

    query = "SELECT count(*) FROM api_document WHERE id = {0} AND pubdate = '{1}-01-01'".format(doc_id, pubdate)
    cur.execute(query)
    if cur.fetchone()[0] > 0:
        print 'Date is correct'
    else:
        query = "UPDATE api_document SET pubdate = '{0}-01-01' WHERE id = {1}".format(pubdate, doc_id)
        cur.execute(query)
        print 'Date updated to {0}-01-01'.format(pubdate)

def do_publisher(doc_id, publisher):
    pub_id = get_publisher_id(publisher)
    if pub_id < 0:
        pub_id = insert_publisher(publisher)
    else:
        print "publisher '{0}' already exists".format(publisher)

    query = "SELECT publisher_id FROM api_document WHERE id = %s"
    cur.execute(query, (doc_id,))
    if pub_id != cur.fetchone()[0]:
        print 'Update publisher id to {0}'.format(pub_id)
        query = "UPDATE api_document SET publisher_id = %s WHERE id = %s"
        cur.execute(query, (pub_id, doc_id))
    else:
        print 'Publisher is correct'

def do_title(doc_id, title):
    query = "SELECT count(*) FROM api_document WHERE id = %s AND title = %s"
    cur.execute(query, (doc_id, title))
    if cur.fetchone()[0] > 0:
        print 'Title is correct'
    else:
        query = "UPDATE api_document SET title = %s WHERE id = %s"
        cur.execute(query, (title, doc_id))
        print 'Title updated to "', title, '"'

def do_url(doc_id, url):
    query = "SELECT count(*) FROM api_document WHERE id = %s AND url = %s"
    cur.execute(query, (doc_id, url))
    if cur.fetchone()[0] > 0:
        print 'URL is correct'
    else:
        query = "UPDATE api_document SET url = %s WHERE id = %s"
        cur.execute(query, (url, doc_id))
        print "URL updated to '{0}'".format(url)

def insert_author(first_name, surname, gender):
    a_id = get_author_id(first_name, surname)
    if a_id == -1:
        query = "INSERT INTO api_author(forenames, surname, gender) VALUES ($${0}$$, $${1}$$, '{2}') RETURNING id".format(
            first_name, surname, gender[0])
        cur.execute(query)
        a_id = cur.fetchone()[0]
        print 'Insert new author: {0}, {1} with id: {2}'.format(first_name, surname, a_id)
    return a_id

def insert_document_author(doc_id, author_id):
    query = "INSERT INTO api_document_author(document_id, author_id) VALUES (%s, %s)"
    cur.execute(query, (doc_id, author_id))

def insert_document_genre(doc_id, genre_id):
    query = "INSERT INTO api_document_genre(document_id, genre_id) VALUES (%s, %s)"
    cur.execute(query, (doc_id, genre_id))

def insert_genre(genre):
    query = "INSERT INTO api_genre(name) VALUES ($${0}$$) RETURNING id".format(genre)
    cur.execute(query)
    g_id = cur.fetchone()[0]
    print 'Insert new genre: {0} with id: {1}'.format(genre, g_id)
    return g_id

def insert_publisher(publisher):
    query = "INSERT INTO api_publisher(name) VALUES (%s) RETURNING id"
    cur.execute(query, (publisher,))
    p_id = cur.fetchone()[0]
    print 'Insert publisher: {0} with id: {1}'.format(publisher, p_id)
    return p_id

data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data')
fp = os.path.join(data_dir, 'Database - Doc level metadata - Data cleaning2.csv')
with open(fp, 'r') as adoc:
    for line in adoc:
        aline = line.split('|')
        doc_id = aline[0]

        if doc_id == 'Primary key':
            continue

        if True:
            a1        = aline[1].strip()
            g1        = aline[2].strip()
            a2        = aline[3].strip()
            g2        = aline[4].strip()
            title     = aline[5].strip()
            pdate     = aline[6].strip()
            publisher = aline[7].strip()
            url       = aline[8].strip()
            genre1    = aline[9].strip()
            genre2    = aline[10].strip()
            genre3    = aline[11].strip()

            print 'Checking document ', doc_id
            do_author(doc_id, a1, g1)
            if a2:
                do_author(doc_id, a2, g2)

            do_title(doc_id, title)
            do_pubdate(doc_id, pdate)

            if publisher:
                do_publisher(doc_id, publisher)

            do_url(doc_id, url)

            do_genre(doc_id, genre1)
            if genre2:
                do_genre(doc_id, genre2)
            if genre3:
                do_genre(doc_id, genre3)
            print '--------------------------'

con.commit()
cur.close()
con.close()
