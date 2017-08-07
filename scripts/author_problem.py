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

authors = {}
UNKOWN = ['Anon.', '"""A Yankee"""', 'Eudora', 'Evangeline', 'Porte', 'Yankee']
fp = os.path.join(data_dir, 'gender.csv')
with open(fp, 'r') as auths:
    for line in auths:
        aline = line.split('|')
        authors[aline[0]] = aline[1]

fp = os.path.join(data_dir, 'Database - Doc level metadata - Data cleaning.csv')

def get_author_id(first_name, surname):
    a_id = None
    query = "SELECT id FROM api_author WHERE forenames = $${0}$$ AND surname = $${1}$$".format(
            first_name, surname)
    cur = con.cursor()
    cur.execute(query)
    res = cur.fetchone()
    cur.close()
    if res and len(res) == 1:
        a_id = res[0]
    else:
        a_id = -1;
    return a_id

def insert_document_author(doc_id, author_id):
    query = "INSERT INTO api_document_author(document_id, author_id) VALUES ({0}, {1})".format(
        doc_id, author_id)
    #cur = con.cursor()
    cur.execute(query)
    #con.commit()
    #cur.close()

def insert_author(name, gender):
    names = name.split(',')
    if len(names) != 2:
        if names[0] in UNKOWN:
            #names[1] = ''
            names.append('')
            gender = 'unknown'
        else:
            print 'Name is not correct:  ', a1
            return -1;
    first_name = names[1]
    surname = names[0]

    if len(name) == 0:
        exit(0)

    a_id = get_author_id(first_name, surname)
    if a_id == -1:
        query = "INSERT INTO api_author(forenames, surname, gender) VALUES ($${0}$$, $${1}$$, '{2}') RETURNING id".format(
            first_name, surname, gender[0])
        cur = con.cursor()
        cur.execute(query)
        a_id = cur.fetchone()[0]
        print 'Insert new author: {0}, {1} with id: {2}'.format(first_name, surname, a_id)
        #con.commit()
        #cur.close()
    return a_id

with open(fp, 'r') as adoc:
    def do_author(doc_id, name, title):
        success = True

        if len(name) > 0:

            if name in authors:
                author_id = insert_author(name, authors[name])
                if author_id < 0:
                    success = False
                else:
                    insert_document_author(doc_id, author_id)
                    print "Insert new api_document_author for {0} => {1}".format(name, title)
            else:
                print 'Author not found:  ', name, len(name)
                success = False
        return success

    for line in adoc:
        aline = line.split('|')
        doc_id = aline[0]
        a1 = aline[1]
        a2 = aline[2]
        title = aline[3]

        if doc_id == 'Primary key':
            continue

        query = "SELECT count(*) FROM api_document_author WHERE document_id = {0}".format(doc_id)
        cur.execute(query)
        if cur.fetchone()[0] == 0:
            #print doc_id, a2
            if not do_author(doc_id, a1, title) or not do_author(doc_id, a2, title):
                break
        else:
            #print doc_id, row
            pass

con.commit()
cur.close()
con.close()
