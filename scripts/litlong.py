#!/usr/bin/env python

from os.path import expanduser

import psycopg2

authors = {}
fp = '{0}/gender.csv'.format(expanduser("~"))
with open(fp, 'r') as auths:
    for line in auths:
        aline = line.split('|')
        authors[aline[0]] = aline[1]

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
query = "DELETE FROM api_author"
cur.execute(query)
query = "DELETE FROM api_document_author"
cur.execute(query)
query = "ALTER SEQUENCE api_author_id_seq RESTART WITH 1"
cur.execute(query)
cur.close()

IDS = {
    "A journey from Edinburgh through parts of North Britain vol. 1": None,
    "A journey from Edinburgh through parts of North Britain vol. 2": None,
    "Beatty's tour in Europe": 125,
    "Complete poetical and prose works of Robert Burns": 145,
    "Essays critical and imaginative vol. 1": None,
    "Essays critical and imaginative vol. 2": None,
    "Essays critical and imaginative vol. 3": None,
    "Essays critical and imaginative vol. 4": None,
    "House of Achendaroch: an old maid's story": 12,
    "London, or, A month at Steven's vol. 1": None,
    "London, or, A month at Steven's vol. 2": None,
    "London, or, A month at Steven's vol. 3": None,
    "Narratives from criminal trials in Scotland vol. 1": None,
    "Narratives from criminal trials in Scotland vol. 2": None,
    "Noctes Ambrosianae vol. 1": None,
    "Noctes Ambrosianae vol. 2": None,
    "Noctes Ambrosianae vol. 3": None,
    "Noctes Ambrosianae vol. 4": None,
    "Noctes Ambrosianae vol. 5": None,
    "St Andrews and elsewhere": 133,
    "Story of a Stolen Heir. A novel vol. 1": None,
    "Story of a Stolen Heir. A novel vol. 2": None,
    "Story of a Stolen Heir. A novel vol. 3": None,
    "The children's fairy geography": 107,
    "The historical works of Sir James Balfour vol. 1": None,
    "The historical works of Sir James Balfour vol. 2": None,
    "The historical works of Sir James Balfour vol. 3": None,
    "The historical works of Sir James Balfour vol. 4": None,
    "The life of Samuel Johnson, LL.D.": 130,
    "The recreations of Christopher North vol. 1": None,
    "The recreations of Christopher North vol. 2": None,
    "The recreations of Christopher North vol. 3": None,
    "The journal of a tour to the Hebrides, with Samuel Johnson, LL.D.": 131,
    "The world's best essays, from the earliest period to the present time vol. 1": None,
    "The world's best essays, from the earliest period to the present time vol. 2": None,
    "The world's best essays, from the earliest period to the present time vol. 3": None,
    "The world's best essays, from the earliest period to the present time vol. 4": None,
    "The world's best essays, from the earliest period to the present time vol. 5": None,
    "The world's best essays, from the earliest period to the present time vol. 6": None,
    "The world's best essays, from the earliest period to the present time vol. 7": None,
    "The world's best essays, from the earliest period to the present time vol. 8": None,
    "The world's best essays, from the earliest period to the present time vol. 9": None,
    "The world's best essays, from the earliest period to the present time vol. 10": None,
    "Varieties in Prose vol. 1" : 1,
    "Varieties in Prose vol. 2":  None,
    "Varieties in Prose vol. 3":  None,
    "Willie Gillies, chapter 1": None,
    "Wilson's historical, traditionary, and imaginative tales of the borders, and of Scotland vol. 1": None,
    "Wilson's historical, traditionary, and imaginative tales of the borders, and of Scotland vol. 2": None,
    "Wilson's historical, traditionary, and imaginative tales of the borders, and of Scotland vol. 3": None,
    "Wilson's historical, traditionary, and imaginative tales of the borders, and of Scotland vol. 4": None,
    "Wilson's historical, traditionary, and imaginative tales of the borders, and of Scotland vol. 5": None,
    "Wilson's historical, traditionary, and imaginative tales of the borders, and of Scotland vol. 6": None,
    "Wilson's tales of the borders and of Scotland": None,
    "Wilson's tales of the borders and Scotland vol. 1": None,
    "Wilson's tales of the borders and Scotland vol. 2": None,
    "Wilson's tales of the borders and Scotland vol. 3": None,
    "Wilson's tales of the borders and Scotland vol. 4": None,
    "Wilson's tales of the borders and Scotland vol. 5": None,
    "Wilson's tales of the borders and Scotland vol. 6": None,
    "Wilson's tales of the borders and Scotland vol. 7": None,
    "Wilson's tales of the borders and Scotland vol. 8": None,
    "Wilson's tales of the borders and Scotland vol. 9": None,
    "Wilson's tales of the borders and Scotland vol. 10": None,
    "Wilson's tales of the borders and Scotland vol. 11": None,
    "Wilson's tales of the borders and Scotland vol. 12": None,
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

    return ids


def get_author_id(first_name, surname):
    a_id = None
    query = "SELECT id FROM api_author WHERE forenames = $$'{0}'$$ AND surname = '{1}'".format(
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

def insert_author(name, gender):

    names = name.split(',')
    if len(names) != 2:
        if names[0] == 'Anon.':
            names[0] = ''
            names.append('Anon');
        else:
            print 'Name is not correct:  ', a1
            return -1;

    first_name = names[0]
    surname = names[1]
    #print first_name, surname, gender

    a_id = get_author_id(first_name, surname)
    if a_id == -1:
        query = "INSERT INTO api_author(forenames, surname, gender) VALUES ($$'{0}'$$, '{1}', '{2}') RETURNING id".format(
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
    cur.close()


fp = '{0}/authors.csv'.format(expanduser("~"))
with open(fp, 'r') as adoc:

    def do_author(name, doc_id):
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

        a1 = aline[0]

        if a1 == 'Author 1':
            continue

        a2 = aline[1]
        a3 = aline[2]
        title = aline[3]
        link = aline[6]

        print '\n','* ',title,' *'

        # get document id
        doc_ids = _get_title_ids(title)

        if len(doc_ids) == 0:
            print 'Title not found: ', title
            break;
        elif doc_ids[0] == None:
            # TODO remove later
            continue

        # TODO use first doc_id
        if not do_author(a1, doc_ids[0]) or not do_author(a2, doc_ids[0]) or not do_author(a3, doc_ids[0]):
            break

con.commit()
con.close()
