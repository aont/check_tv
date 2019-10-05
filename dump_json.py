#!/usr/bin/env python3

import feedparser
import sys
import datetime
import psycopg2
import json
import os
import requests

def str_abbreviate(str_in):
    len_str_in = len(str_in)
    if len_str_in > 128*2+10:
        return str_in[0:128] + " ... " + str_in[-128:]
    else:
        return str_in

def pg_execute(pg_cur, query, param=None):
    param_str = str_abbreviate("%s" % param)
    sys.stderr.write(u'[info] postgres: %s param=%s\n' % (query, param_str))
    return pg_cur.execute(query, param)

def pg_init_json(pg_cur, table_name, key_name):
    pg_result = pg_execute(pg_cur, u"select 1 from pg_tables where schemaname='public' and tablename=%s ;", [table_name])
    pg_result = pg_cur.fetchone()
    if pg_result is None:
        #sys.stderr.write(u'[info] creating table\n')
        pg_execute(pg_cur, u"create table %s (key text unique, value text);" % (table_name))
    elif 1 != pg_result[0] :
        raise Exception(u"exception")

    pg_execute(pg_cur, u'select value from %s where key=%%s;' % table_name, [key_name])
    pg_result = pg_cur.fetchone()
    
    if pg_result is None:
        pg_execute(pg_cur, u'insert into %s VALUES (%%s, %%s);' % table_name, [key_name, u"{}"])
        pg_data = {}
    else:
        sys.stderr.write(u'[info] data=%s\n' % str_abbreviate(pg_result[0]))
        pg_data = json.loads(pg_result[0])
    return pg_data

def pg_update_json(pg_cur, table_name, key_name, pg_data):
    return pg_execute(pg_cur, u'update %s set value = %%s where key = %%s;' % table_name, [json.dumps(pg_data), key_name])

if __name__ == u'__main__':

    pg_url = os.environ[u'DATABASE_URL']
    table_name = u'generic_text_data'
    key_name = u'check_tv'
    pg_conn = psycopg2.connect(pg_url)
    pg_cur = pg_conn.cursor()
    
    check_tv_data = pg_init_json(pg_cur, table_name, key_name)
    sys.stdout.write(json.dumps(check_tv_data, ensure_ascii=False, indent=2, sort_keys=True, separators=(',', ': ')))

    pg_cur.close()
    pg_conn.commit()
    pg_conn.close()