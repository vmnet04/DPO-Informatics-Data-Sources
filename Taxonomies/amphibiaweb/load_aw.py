#!/usr/bin/env python3
#
# Import AW taxonomy

import psycopg2

import simplejson as json
import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs
import pandas as pd
import sys
import csv
import io

import settings


try:
    conn = psycopg2.connect(host=settings.pg_host,
                            database=settings.pg_db,
                            user=settings.pg_user,
                            password=settings.pg_password)
except psycopg2.Error as e:
    print(e)
    sys.exit(1)

conn.autocommit = True
conn.set_client_encoding('UTF8')

cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

thes_id = 'aw_amphibians'


cur.execute("delete from th_thesaurus WHERE thesaurus_id = %s", (thes_id,))
cur.execute("INSERT INTO th_thesaurus (thesaurus_id, thesaurus_name, thesaurus_type) VALUES (%s, "
            "'AmphibiaWeb (2021)', 'science')", (thes_id,))


cur.execute("delete from th_ranks WHERE thesaurus_id = %s", (thes_id, ))

cur.execute("INSERT INTO th_ranks (thesaurus_id, rank_name) VALUES (%s, 'Order') "
                      "RETURNING rank_id", (thes_id, ))
l0_rank = cur.fetchone()['rank_id']

cur.execute("INSERT INTO th_ranks (thesaurus_id, rank_name) VALUES (%s, 'Family') "
                      "RETURNING rank_id", (thes_id, ))
l1_rank = cur.fetchone()['rank_id']

cur.execute("INSERT INTO th_ranks (thesaurus_id, rank_name) VALUES (%s, 'Subfamily') "
                      "RETURNING rank_id", (thes_id, ))
l2_rank = cur.fetchone()['rank_id']

cur.execute("INSERT INTO th_ranks (thesaurus_id, rank_name) VALUES (%s, 'Genus') "
                      "RETURNING rank_id", (thes_id, ))
l3_rank = cur.fetchone()['rank_id']

cur.execute("INSERT INTO th_ranks (thesaurus_id, rank_name) VALUES (%s, 'Subgenus') "
                      "RETURNING rank_id", (thes_id, ))
l4_rank = cur.fetchone()['rank_id']

cur.execute("INSERT INTO th_ranks (thesaurus_id, rank_name) VALUES (%s, 'Species') "
                      "RETURNING rank_id", (thes_id, ))
l5_rank = cur.fetchone()['rank_id']


filename = 'amphib_names.tsv'


query_elements = "INSERT INTO th_elements (element_name, element_parent, rank_id) VALUES " \
                 "  (%(element_name)s, %(element_parent)s, %(rank_id)s)" \
                 "ON CONFLICT (element_name, rank_id) DO UPDATE SET element_name = %(element_name)s RETURNING element_id"


with open(filename, newline='', encoding='utf-8') as csvfile:
    datareader = csv.reader(csvfile, delimiter='\t')
    for row in datareader:
        #0
        cur.execute(query_elements, {'element_name': row[0] , 'element_parent': AsIs("NULL"), 'rank_id': l0_rank})
        parent_id = cur.fetchone()['element_id']
        print(row[0])
        #1
        cur.execute(query_elements, {'element_name': row[1], 'element_parent': parent_id, 'rank_id': l1_rank})
        parent_id = cur.fetchone()['element_id']
        print(row[1])
        # 2
        if row[2] != '':
            cur.execute(query_elements, {'element_name': row[2], 'element_parent': parent_id, 'rank_id': l2_rank})
            parent_id = cur.fetchone()['element_id']
            print(row[2])
        # 3
        cur.execute(query_elements, {'element_name': row[3], 'element_parent': parent_id, 'rank_id': l3_rank})
        parent_id = cur.fetchone()['element_id']
        print(row[3])
        # 4
        if row[4] != '':
            cur.execute(query_elements, {'element_name': row[4], 'element_parent': parent_id, 'rank_id': l4_rank})
            parent_id = cur.fetchone()['element_id']
            print(row[4])
        # species
        cur.execute(query_elements, {'element_name': row[5], 'element_parent': parent_id, 'rank_id': l5_rank})
        species_id = cur.fetchone()['element_id']
        print(row[5])
        # cm name
        if len(row) > 6:
            cur.execute(query_elements, {'element_name': row[6], 'element_parent': parent_id, 'rank_id': l6_rank})
            parent_id = cur.fetchone()['element_id']
            print(row[6])
        else:
            print("Row ended.")
            continue




cur.close()
conn.close()

