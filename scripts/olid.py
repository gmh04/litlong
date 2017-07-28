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
# with open(fp, 'r') as pgpass:
#     for line in pgpass:
#         aline = line.split(':')
#         if aline[2] == db['NAME']:
#             db['HOST'] = aline[0]
#             db['PORT'] = aline[1]
#             db['USER'] = aline[3]
#             db['PASS'] = aline[4].strip()
#             break
con = psycopg2.connect(
   "dbname='{NAME}' user='{USER}' host='{HOST}' port={PORT} password='{PASS}'".format(**db))


# query = "SELECT id, forenames, surname FROM api_author"
cur = con.cursor()
# cur.execute(query)
# for i in cur.fetchall():
#     forenames = i[1]
#     surname = i[2]
#     #print surname
#     pass


doc_2_author = {
    '61': '46',
    '308': '218',
    '525': '324'
}

data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data')
fp = os.path.join(data_dir, 'Database - Doc level metadata - Data cleaning.csv')




with open(fp, 'r') as adoc:


    def get_keys(entry, author_surnames):
        keys = {'authors': []}

        #print '-> ', author_surnames
        for doc in entry['docs']:
            if 'author_name' not in doc:
                continue
            else:
                #print doc['author_name']
                pass
            #print '--> ', doc['author_name']
            for i, author in enumerate(doc['author_name']):
                authors = []
                #print author.lower()
                for surname in author_surnames:
                    if surname in author.lower():
                        if 'doc' not in keys:
                            #print doc
                            keys['doc'] = 'https://openlibrary.org{0}'.format(doc['key'])

                        a_id = doc['author_key'][i]
                        keys['authors'].append({
                            'id': a_id,
                            'url': 'https://openlibrary.org/authors/{0}'.format(a_id),
                            'surname': surname
                        })

            # just return first
            if 'doc' in keys:
                return keys

    def update_db(doc_id, doc_url, author):
        #print doc_id, doc_url
        doc_ol_id = doc_url.split('works/')[1]
        author_ol_id = author['id']
        print doc_id, doc_url, author['url'], doc_ol_id, author_ol_id, author['surname']

        #
        #print type(doc_ol_id)
        query = "UPDATE api_document SET ol_id = %s WHERE id = %s"
        cur.execute(query, (doc_ol_id, doc_id))

        #print doc_id, doc_id in doc_2_author
        if doc_id in doc_2_author:
            query = "UPDATE api_author SET ol_id = %(ol_id)s WHERE id = %(a_id)s"
            cur.execute(query,
                {
                    'ol_id': author_ol_id,
                    'a_id': doc_2_author[doc_id]
                }
            )
        else:
            query = "UPDATE api_author SET ol_id = %(a_id)s WHERE id = (SELECT a.id FROM api_author a, api_document d, api_document_author da WHERE d.id = da.document_id AND da.author_id = a.id AND d.id = %(d_id)s AND lower(a.surname) = %(surname)s)"
            cur.execute(query,
                {
                    'a_id': author_ol_id,
                    'd_id': doc_id,
                    'surname': author['surname']
                }
            )

        con.commit()

    for i in xrange(0):
        adoc.next()
    for line in adoc:
        aline = line.split('|')

        doc_id = aline[0]
        a1 = aline[1]

        if a1 == 'Author 1':
            continue

        a2 = aline[2]
        title = aline[3]
        date = aline[5]
        link = aline[6]
        g1 = aline[8]
        g2 = aline[9]
        g3 = aline[10]
        publisher = aline[11]

        author_surnames = [a1.lower().split(',')[0]]
        if a2 is not None and len(a2) > 0:
            author_surnames.append(a2.lower().split(',')[0])
        #print author_surnames
        #exit(0)

        url = 'http://openlibrary.org/search.json'
        payload = {'title' : title.split(':')[0]}

        surname = a1.split(',')[0]
        #print '\n',title, ' : ',  surname
        r = requests.get(url, params=payload)
        if r.status_code == 200:
            entry = r.json()
            doc_url = ''
            author_url = ''
            if entry['num_found'] > 0:
                #print entry
                keys = get_keys(entry, author_surnames)
                if keys:
                    doc_url = keys['doc']
                    author = keys['authors'][0]
                    author_url = author['url']
                    update_db(doc_id, doc_url, author)
            #print doc_id, '|' , title, '|', entry['num_found'], '|', doc_url, '|', author_url

        else:
            print doc_id, '|' , title, '|', 0, '|' ,'error'

        if doc_id == '-63':
            exit(0)
        #print '\n'

cur.close()
con.close()
