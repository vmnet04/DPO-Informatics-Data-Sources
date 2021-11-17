#!/usr/bin/env python3
#
# Import NMAI CT

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

thes_id = 'nmai_ct'


cur.execute("delete from th_thesaurus WHERE thesaurus_id = %s", (thes_id,))
cur.execute("INSERT INTO th_thesaurus (thesaurus_id, thesaurus_name, thesaurus_type) VALUES (%s, "
            "'NMAI Cultural Thesaurus', 'culture')", (thes_id,))


cur.execute("delete from th_ranks WHERE thesaurus_id = %s", (thes_id, ))

cur.execute("INSERT INTO th_ranks (thesaurus_id, rank_name) VALUES ('nmai_ct', 'L1 - Continent') "
                      "RETURNING rank_id")
l1_rank = cur.fetchone()['rank_id']

cur.execute("INSERT INTO th_ranks (thesaurus_id, rank_name) VALUES ('nmai_ct', 'L2 - Culture Area') "
                      "RETURNING rank_id")
l2_rank = cur.fetchone()['rank_id']

cur.execute("INSERT INTO th_ranks (thesaurus_id, rank_name) VALUES ('nmai_ct', 'L3 - Sub-Culture Area') "
                      "RETURNING rank_id")
l3_rank = cur.fetchone()['rank_id']

cur.execute("INSERT INTO th_ranks (thesaurus_id, rank_name) VALUES ('nmai_ct', 'L4 - Culture') "
                      "RETURNING rank_id")
l4_rank = cur.fetchone()['rank_id']

cur.execute("INSERT INTO th_ranks (thesaurus_id, rank_name) VALUES ('nmai_ct', 'L5 - Sub-Culture') "
                      "RETURNING rank_id")
l5_rank = cur.fetchone()['rank_id']

cur.execute("INSERT INTO th_ranks (thesaurus_id, rank_name) VALUES ('nmai_ct', 'L6 - Community') "
                      "RETURNING rank_id")
l6_rank = cur.fetchone()['rank_id']


filename = 'NMAI_CT/NMAICultureThesaurus_HierarchyExport_08112021.csv'


query_elements = "INSERT INTO th_elements (element_name, element_parent, rank_id) VALUES " \
                 "  (%(element_name)s, %(element_parent)s, %(rank_id)s)" \
                 "ON CONFLICT (element_name, rank_id) DO UPDATE SET element_name = %(element_name)s RETURNING element_id"


with open(filename, newline='', encoding='utf-8') as csvfile:
    datareader = csv.reader(csvfile)
    for row in datareader:
        cur.execute(query_elements, {'element_name': row[1] , 'element_parent': AsIs("NULL"), 'rank_id': l1_rank})
        parent_id = cur.fetchone()['element_id']
        print(row[1])
        print(len(row))
        # 2
        if len(row) > 2:
            cur.execute(query_elements, {'element_name': row[2], 'element_parent': parent_id, 'rank_id': l2_rank})
            parent_id = cur.fetchone()['element_id']
            print(row[2])
        else:
            print("Row ended.")
            continue
        # 3
        if len(row) > 3:
            cur.execute(query_elements, {'element_name': row[3], 'element_parent': parent_id, 'rank_id': l3_rank})
            parent_id = cur.fetchone()['element_id']
            print(row[3])
        else:
            print("Row ended.")
            continue
        # 4
        if len(row) > 4:
            cur.execute(query_elements, {'element_name': row[4], 'element_parent': parent_id, 'rank_id': l4_rank})
            parent_id = cur.fetchone()['element_id']
            print(row[4])
        else:
            print("Row ended.")
            continue
        # 5
        if len(row) > 5:
            cur.execute(query_elements, {'element_name': row[5], 'element_parent': parent_id, 'rank_id': l5_rank})
            parent_id = cur.fetchone()['element_id']
            print(row[5])
        else:
            print("Row ended.")
            continue
        # 6
        if len(row) > 6:
            cur.execute(query_elements, {'element_name': row[6], 'element_parent': parent_id, 'rank_id': l6_rank})
            parent_id = cur.fetchone()['element_id']
            print(row[6])
        else:
            print("Row ended.")
            continue



cur.close()
conn.close()
