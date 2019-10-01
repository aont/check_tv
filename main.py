#!/usr/bin/env python3

import feedparser
import sys
import datetime
import psycopg2
import json
import os
import requests
import urllib.parse

class LINE(object):
    def __init__(self, sess, line_notify_token):
        self.sess = sess
        self.line_notify_token = line_notify_token
        self.line_notify_api = u'https://notify-api.line.me/api/notify'
        self.headers = {u'Authorization': u'Bearer ' + line_notify_token}

    def notify(self, message):
        # print message
        # return
        sys.stderr.write(u'[info] line notify: %s\n' % message)
        for t in range(5):
            try:
                line_notify = self.sess.post(self.line_notify_api, data = {u'message': message}, headers = self.headers)
                if requests.codes.ok != line_notify.status_code:
                    sys.stderr.write(u"[info] line status_code = %s\n" % line_notify.status_code)
                    sys.stderr.write(u"[info] wait for 5s and retry\n")
                    # sys.stderr.flush()
                    time.sleep(5)
                    continue

                break
            except requests.exceptions.ConnectionError as e:
                sys.stderr.write(u"[warn] LINE ConnectionError occured. retrying...\n")
                sys.stderr.write(traceback.format_exc())
                # sys.stderr.flush()
                continue

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

def keyword2rss(keyword_list):
    rss_list = []
    base_url = u"https://tv.so-net.ne.jp/rss/schedulesBySearch.action?condition.genres%5B0%5D.parentId=-1&condition.genres%5B0%5D.childId=-1&submit=%E6%A4%9C%E7%B4%A2&stationAreaId=23&submit.x=&submit.y="
    for keyword in keyword_list:
        rss_list.append(base_url + "&condition.keyword=%s&stationPlatformId=%s" % (urllib.parse.quote(keyword, safe=''), 1))
        rss_list.append(base_url + "&condition.keyword=%s&stationPlatformId=%s" % (urllib.parse.quote(keyword, safe=''), 2))
    return rss_list

if __name__ == u'__main__':

    line_sess = requests.session()
    line_notify_token = os.environ[u'LINE_TOKEN']
    line = LINE(line_sess, line_notify_token)

    tv_sonet_base = u"https://tv.so-net.ne.jp/rss/schedulesBySearch.action?condition.genres%5B0%5D.parentId=-1&condition.genres%5B0%5D.childId=-1&submit=%E6%A4%9C%E7%B4%A2&stationAreaId=23&submit.x=&submit.y="

    pg_url = os.environ[u'DATABASE_URL']
    table_name = u'generic_text_data'
    key_name = u'check_tv'
    pg_conn = psycopg2.connect(pg_url)
    pg_cur = pg_conn.cursor()

    check_tv_data = pg_init_json(pg_cur, table_name, key_name)
    checked_previously = check_tv_data.get(u"checked_previously")
    if checked_previously is None:
        checked_previously = []
    checked_thistime = []

    keyword_list = check_tv_data.get(u"keyword_list")
    if keyword_list is None:
        keyword_list = []

    for rssurl in keyword2rss(keyword_list):
        sys.stderr.write(u'[info] rss=%s\n' % rssurl) 
        d = feedparser.parse(rssurl)

        for entry in d['entries']:
            checked_thistime.append(entry.link)
            if entry.link not in checked_previously:
                line.notify(u"%s\n%s" % (entry.title, entry.link))
        
    check_tv_data[u'checked_previously'] = checked_thistime
    pg_update_json(pg_cur, table_name, key_name, check_tv_data)
    
    pg_cur.close()
    pg_conn.commit()
    pg_conn.close()