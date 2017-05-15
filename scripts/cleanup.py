#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os.path import expanduser

import os
import psycopg2

data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data')

authors = {}
UNKOWN = ['Anon.', '"""A Yankee"""', 'Eudora', 'Evangeline', 'Porte', 'Yankee']
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
    "Willie Gillies, chapter 1": 492,
    "The historical works of Sir James Balfour vol. 2": 115,
    "Beatty's tour in Europe": 125,
    "Story of a Stolen Heir. A novel vol. 1": 8,
    #"Story of a Stolen Heir. A novel vol. 2": None,
    #"Story of a Stolen Heir. A novel vol. 3": None,
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
    "Miss Armstrong's and other circumstances": 27,
    "Benedict's wanderings": 186,
    "Benedict's wanderings in Ireland, Scotland, Italy, & Sicily": 187,
    "A tour thro' the whole island of Great Britain vol. 1-3": 507,
    "The Cleekim Inn": 28,
    "Diary, sketches, and reviews": 192,
    "Observations in Europe, principally in France and Great Britain vol. 2": 33,
    "Marriage vol. 1": 195,
    #"A Californian circling the globe": None,
    "Ringan Gilhaize: or, The covenanters vol. 1": 204,
    "Ringan Gilhaize: or, The covenanters vol. 2": 203,
    "Ringan Gilhaize: or, The covenanters vol. 3": 205,
    "Allan Breck vol. 2": 207,
    "Christopher North: a memoir of John Wilson": 209,
    "The fortunes of the Falconars vol. 2": 210,
    "Annie Jennings vol. 1": 211,
    "Annie Jennings vol. 2": 212,
    "Colville of the guards vol. 1": 220,
    "The Romance of War": 40,
    "The King's own borderers vol. 1": 222,
    "The master of Aberfeldie vol. 2": 218,
    "The white cockade vol. 2": 219,
    "Letters of Asa Gray vol. 1": 223,
    "Letters and papers relating to Patrick, Master of Gray, afterwards seventh [sixth] Lord Gray": 496,
    "A Year in Europe vol. 2": 226,
    "A Traveller's Notes, in Scotland, Belgium, Devonshire, the Channel Islands, the Mediterranean, France, Somersetshire, Cornwall, the Scilly Islands, Wilts, and Dorsetshire": 36,
    "Memoirs of the life and writings of Thomas Chalmers vol. 2": 230,
    "Memoirs of the life and writings of Thomas Chalmers vol. 4": 231,
    "The Story of My Life vol. 4-6": 522,
    "Passages from the English Note-books of Nathaniel Hawthorne vol. 2": 46,
    "Memoirs of William Hazlitt vol. 2": 239,
    "Memoirs and correspondence of Francis Horner, M.P. vol. 1": 244,
    "Life and Correspondence of David Hume vol. 1": 517,

    "Elizabeth de Bruce vol. 1": 260,
    "Elizabeth de Bruce vol. 2": 262,
    "Elizabeth de Bruce vol. 3": 258,
    "The Edinburgh tales vol. 1": 263,
    "The Edinburgh tales vol. 2": 261,
    "The Edinburgh tales vol. 3": 259,
    "Memoirs of Sir William Knighton vol. 1": 265,
    "The works of John Knox vol. 2": 267,
    "Memoirs of Charles Lee Lewes vol. 3": 271,
    "Memoirs of Charles Lee Lewes vol. 4": 270,
    "Travels in land beyond the sea": 272,
    #"Life of Walter Scott": None,
    "Memoirs of the life of Sir Walter Scott, Bart. vol. 4-6": 279,
    "Memoirs of the life of Sir Walter Scott, Bart. vol. 7-9": 277,
    "Peter's letters to his kinsfolk vol. 1": 278,
    "Peter's letters to his kinsfolk vol. 2": 276,
    "Peter's letters to his kinsfolk vol. 3": 280,
    "Edinburgh": 285,
    "Queen's Maries. A romance of Holyrood vol. 2": 63,
    "Rambles in Europe": 292,
    "Edinburgh and its neighbourhood, geological & historical": 293,
    "The Provost-Marshal: a romance of the middle shires": 64,
    "Memoirs, journals, and correspondence of Thomas Moore vol. 5": 299,
    "Memoirs, journals, and correspondence of Thomas Moore vol. 8": 297,
    "A year in Europe": 298,
    "Rambles in Europe": 66,

    "Andrew Ramsay of Errol vol. 1": 71,
    "The Black Watch vol. 1": 321,
    "The Black Watch vol. 3": 322,
    "The White House at Inch Gow": 70,
    "Aground in the shallows vol. 1": 72,
    "Christie Johnstone: a novel": 73,
    "The Itinerant, in Scotland": 332,
    #"The Fortunes of Nigel": None,
    #"Guy Mannering": None,
    #"Tales of a Grandfather": None,
    "The Abbot vol. 1": 548,
    #"The antiquary": None,
    "Waverley vols. 1-3": 558,
    #"The journal of Walter Scott": None,
    "The Weird of the Wentworths; A Tale of George IV's Time vol. 1": 534,
    #"Haco the dreamer vol. 1": None,
    #"Haco the dreamer vol. 2": None,
    "A journal of travels in England, Holland and Scotland vol. 3": 347,
    "Journal of a tour and residence in Great Britain vol. 2": 349,
    #"R. L. S. - Some Edinburgh Notes": None,
    #"Holiday House": None,
    "Modern Flirtations vol. 2": 354,

    "The library of choice literature vol. 1": 403,
    "Mr Peters": 365,
    "Sunny memories of foreign lands vol. 1": 381,
    "The British isles": 391,
    "The underground city (also pub. as 'The Child of the Cavern')": 545,
    "His Dearest Wish vol. 2": 103,
    #"A Memphian's trip to Europe with Cook's educational party": None,
    "A journal of a residence during several months in London": 104,
    #"Nelly Armstrong: a story of the day vol. 1": None,
    #"Nelly Armstrong: a story of the day vol. 2": None,
    "Penelope's Progress, chapters 8-11": 105,
    "Penelope's Progress": 414,
    "Penelope's Experiences in Scotland": 546,
    "Memorials of Edinburgh in olden times": 106,
    #"The Works of Professor Wilson vol. 11: Tales": None,
    "Wilson's tales of the borders and Scotland vol. 1": 422,
    "Wilson's tales of the borders and Scotland vol. 11": 433,
    "Wilson's tales of the borders and Scotland vol. 12": 435,
    "Wilson's tales of the borders and Scotland vol. 2": 427,
    "Wilson's tales of the borders and Scotland vol. 3": 428,
    "Wilson's tales of the borders and Scotland vol. 4": 425,
    "Wilson's tales of the borders and Scotland vol. 5": 429,
    #"Wilson's tales of the borders and Scotland vol. 6": None,
    "Wilson's tales of the borders and Scotland vol. 7": 432,
    #"Essays Critical and Imaginative vol. 1": None,
    #"Essays Critical and Imaginative vol. 2": None,
    "The children's fairy geography": 107,
    "Critical and miscellaneous essays vol. 1": 443,
    "Noctes Ambrosianœ vol. 1": 437,
    "Noctes Ambrosianœ vol. 2": 444,
    "Noctes Ambrosianœ vol. 3": 431,
    "Noctes Ambrosianœ vol. 4": 442,
    #"Lucy, Francis and Cousin Bill": None,
}


def _get_title_ids(title):
    ids = []

    if title in IDS:
        ids.append(IDS[title])
    else:
        query = "SELECT id FROM api_document WHERE LOWER(title) like $${0}%$$".format(
            title.lower())
        cur = con.cursor()
        cur.execute(query)
        for i in cur.fetchall():
            ids.append(i[0])
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
    query = "UPDATE api_document SET pubdate = '{0}-01-01' WHERE id = {1}".format(
        date, doc_id)
    cur = con.cursor()
    cur.execute(query)
    con.commit()
    cur.close()


def update_document_url(doc_id, url):
    query = "UPDATE api_document SET url = '{0}' WHERE id = {1}".format(
        url, doc_id)
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


fp = os.path.join(data_dir, 'deletions.csv')
with open(fp, 'r') as adoc:
    cur = con.cursor()

    for line in adoc:
        aline = line.split('|')

        pk = aline[0]

        if pk == 'Primary key':
            continue

        query = "UPDATE api_document SET active = FALSE WHERE id = {0}".format(pk)
        cur.execute(query)

    con.commit()
    cur.close()

#fp = os.path.join(data_dir, 'authors.csv')
fp = os.path.join(data_dir, 'Database - Doc level metadata - Data cleaning.csv')
with open(fp, 'r') as adoc:

    def do_author(doc_id, name):
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

        if len(doc_id) > 0 and doc_id[0] == '?':
            print '\n','*** skip: ',title,' ***'
            continue

        print '\n','* ', doc_id, title,' *'

        # get document id
        if len(doc_id) == 0:
            doc_ids = _get_title_ids(title)

            if len(doc_ids) == 0:
                print 'Title not found: ', title
                exit(0)
            elif doc_ids[0] == None:
                print 'Title null {0}'.format(title)
                exit(0)
            elif len(doc_ids) > 1:
                print 'More than one title found for {0}'.format(title)
                exit(0)
            doc_id = doc_ids[0]

        if len(date) == 4:
            if date == '????':
                print 'Skip date for ', title
            else:
                update_document_date(doc_id, date)

        if publisher != None and len(publisher) > 0:
            update_document_publisher(doc_id, publisher);

        if link != None and len(link) > 0:
            update_document_url(doc_id, link);

        if not do_author(doc_id, a1) or not do_author(doc_id, a2):
            break

        if not do_genre(doc_id, g1) or not do_genre(doc_id, g2) or not do_genre(doc_id, g3):
            break


con.commit()
con.close()
