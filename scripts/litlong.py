#!/usr/bin/env python

from os.path import expanduser

import os
import psycopg2

data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data')

authors = {}
UNKOWN = ['Anon.', '"""A Yankee"""', 'Eudora', 'Evangeline', 'Porte']
fp = os.path.join(data_dir, 'gender.csv')
with open(fp, 'r') as auths:
    for line in auths:
        aline = line.split('|')
        authors[aline[0]] = aline[1]
    #authors['"""A Yankee"""'] = ''

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
query = "DELETE FROM api_document_author"
cur.execute(query)
query = "DELETE FROM api_author"
cur.execute(query)
query = "ALTER SEQUENCE api_author_id_seq RESTART WITH 1"
cur.execute(query)
query = "DELETE FROM api_document_genre"
cur.execute(query)
query = "DELETE FROM api_genre"
cur.execute(query)
query = "ALTER SEQUENCE api_genre_id_seq RESTART WITH 1"
cur.execute(query)

IDS = {
    "Varieties in Prose vol. 2": 1,
    "Willie Gillies, chapter 1": None,
    "The historical works of Sir James Balfour vol. 2": 115,
    "Beatty's tour in Europe": 125,
    "Story of a Stolen Heir. A novel vol. 1": None,
    "Story of a Stolen Heir. A novel vol. 2": None,
    "Story of a Stolen Heir. A novel vol. 3": None,
    "Broken stowage": 129,
    "The journal of a tour to the Hebrides, with Samuel Johnson, LL.D.": 131,
    "The life of Samuel Johnson, LL.D.": 130,
    "St Andrews and elsewhere": 133,
    "The world's best essays, from the earliest period to the present time vol. 2": 134,
    "Spare hours": 136,
    "Spare hours vol. 2": 137,
    "London, or, A month at Steven's vol. 1": 138,
    "Complete poetical and prose works of Robert Burns": 145,
    "Narratives from criminal trials in Scotland vol. 2": 146,
    "House of Achendaroch: an old maid's story": 12,
    "A journey from Edinburgh through parts of North Britain vol. 1": 148,
    "Reminiscences": 150,
    "American Four-in-Hand in Britain": 152,
    "Memoirs of Robert Chambers": 158,
    "The life of Joseph Hodges Choate as gathered chiefly from his letters vol. 2": 162,
    "Abroad: Journal of a Tour Through Great Britain and on the Continent": 16,
    "Correspondence of James Fenimore-Cooper vol. 1": 170,
    "A noble life vol. 2": 173,
    "The Stickit minister": 26,
    "Oliver Cromwell's letters and speeches: with elucidations vol. 1": 180,
    "North of the Tweed vol. 3": 182,
    "Europe through a woman's eye": 183,


    "A Californian circling the globe": None,
    "A Memphian's trip to Europe with Cook's educational party": None,
    "Guy Mannering": None,
    "Haco the dreamer vol. 1": None,
    "Haco the dreamer vol. 2": None,
    "Holiday House": None,
    "Life of Walter Scott": None,
    "Lucy, Francis and Cousin Bill": None,
    "Nelly Armstrong: a story of the day vol. 1": None,
    "Nelly Armstrong: a story of the day vol. 2": None,
    "R. L. S. - Some Edinburgh Notes": None,
    "Tales of a Grandfather": None,
    "The antiquary": None,
    "The Fortunes of Nigel": None,
    "The journal of Walter Scott": None,
    "Wilson's tales of the borders and Scotland vol. 6": None,
}


def _get_title_ids(title):
    ids = []

    if title in IDS:
        ids.append(IDS[title])
    else:
        query = "SELECT id FROM api_document WHERE LOWER(title) like $${0}%$$".format(
            title.lower())
        print query
        cur = con.cursor()
        cur.execute(query)
        for i in cur.fetchall():
            ids.append(i[0])
        #print ids
    return ids


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


def get_publisher_id(name):
    a_id = None
    query = "SELECT id FROM api_publisher WHERE name = '{0}'".format(name)
    cur = con.cursor()
    cur.execute(query)
    res = cur.fetchone()
    cur.close()
    if res and len(res) == 1:
        a_id = res[0]
    else:
        a_id = -1;
    return a_id


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


def insert_author(name, gender):
    names = name.split(',')
    print names
    if len(names) != 2:
        if names[0] in UNKOWN:
            #names[1] = ''
            names.append('')
            gender = 'unknown'
        else:
            print 'Name is not correct:  ', a1
            return -1;
    print names
    first_name = names[1]
    surname = names[0]
    print first_name, surname, gender

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
        con.commit()
        cur.close()
    return a_id


def insert_document_author(doc_id, author_id):
    query = "INSERT INTO api_document_author(document_id, author_id) VALUES ({0}, {1})".format(
        doc_id, author_id)
    cur = con.cursor()
    cur.execute(query)
    con.commit()
    cur.close()


def update_document_date(doc_id, date):
    query = "UPDATE api_document SET pubdate = '{0}-01-02' WHERE id = {1}".format(
        date, doc_id)
    cur = con.cursor()
    cur.execute(query)
    con.commit()
    cur.close()


def update_document_publisher(doc_id, name):
    pub_id = get_publisher_id(name)

    cur = con.cursor()

    if pub_id == None or pub_id == -1:
        query = "INSERT INTO api_publisher(name) VALUES ('{0}') RETURNING id".format(name)
        cur.execute(query)
        pub_id = cur.fetchone()[0]
        print 'Insert new publisher: {0} with id: {1}'.format(name, pub_id)
    query = "UPDATE api_document SET publisher_id = '{0}' WHERE id = {1}".format(pub_id, doc_id)
    cur.execute(query)
    con.commit()
    cur.close()


#fp = os.path.join(data_dir, 'authors.csv')
fp = os.path.join(data_dir, 'Database - Doc level metadata - Data cleaning.csv')
with open(fp, 'r') as adoc:

    def do_author(doc_id, name):
        success = True

        if len(name) > 0:

            # TODO remove this
            name = name.replace('(ed.)', '')
            name = name.strip()

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

    def do_genre(doc_id, genre):
        success = True

        if len(genre) > 0:
            cur = con.cursor()

            get_genre_id
            g_id = get_genre_id(genre)
            if g_id == -1:

                query = "INSERT INTO api_genre(name) VALUES ($${0}$$) RETURNING id".format(genre)
                cur.execute(query)
                g_id = cur.fetchone()[0]
                #con.commit()
                print 'Insert new genre: {0} with id: {1}'.format(genre, g_id)

            query = "INSERT INTO api_document_genre(document_id, genre_id) VALUES ({0}, {1})".format(doc_id, g_id)
            cur.execute(query)
            print "Insert new api_document_genre for {0} => {1} {2}".format(doc_id, g_id, genre)

            con.commit()
            cur.close()
        return success

    for i in xrange(0):
        adoc.next()
    for line in adoc:
        aline = line.split('|')

        doc_id = ''#aline[0]
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

        # TODO: remove
        if len(title) == 0:
            print '\n','*** skip: empty title ***'
            continue

        if len(doc_id) > 0 and doc_id[0] == '?':
            print '\n','*** skip: ',title,' ***'
            continue

        print '\n','* ',title,' *'

        # get document id
        if len(doc_id) == 0:
            doc_ids = _get_title_ids(title)

            if len(doc_ids) == 0:
                print 'Title not found: ', title
                exit(0)
            elif doc_ids[0] == None:
                # TODO remove later
                continue
                #print 'Title null {0}'.format(title)
                #exit(0)
            elif len(doc_ids) > 1:
                print 'More than one title found for {0}'.format(title)
                exit(0)
            doc_id = doc_ids[0]

        if len(date) == 4:
            update_document_date(doc_id, date);

        if publisher != None and len(publisher) > 0:
            update_document_publisher(doc_id, publisher);
            #exit(0)

        if not do_author(doc_id, a1) or not do_author(doc_id, a2):
            break

        if not do_genre(doc_id, g1) or not do_genre(doc_id, g2) or not do_genre(doc_id, g3):
            break

        # remove
        con.commit()

con.commit()
con.close()
