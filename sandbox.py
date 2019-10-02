#!/usr/bin/env python3

import feedparser
import sys
import datetime
import psycopg2
import json
import os
import requests
import urllib.parse


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
        rss_list.append(base_url + "&condition.keyword=%s&stationPlatformId=%s" % (urllib.parse.quote(keyword, safe=''), 0))
        # rss_list.append(base_url + "&condition.keyword=%s&stationPlatformId=%s" % (urllib.parse.quote(keyword, safe=''), 1))
        # rss_list.append(base_url + "&condition.keyword=%s&stationPlatformId=%s" % (urllib.parse.quote(keyword, safe=''), 2))
    return rss_list

def check_channel(summary):
    filter_channels = ["ＮＨＫ総合１・東京(Ch.1)","ＮＨＫＥテレ１・東京(Ch.2)","日テレ(Ch.4)","テレビ朝日(Ch.5)","ＴＢＳ(Ch.6)","テレビ東京(Ch.7)","フジテレビ(Ch.8)","ＴＯＫＹＯ　ＭＸ１(Ch.9)",\
        "ＮＨＫ ＢＳ１(Ch.1)","ＮＨＫ ＢＳプレミアム(Ch.3)","ＢＳ日テレ(Ch.4)","ＢＳ朝日(Ch.5)","ＢＳ-ＴＢＳ(Ch.6)","ＢＳテレ東(Ch.7)","ＢＳフジ(Ch.8)","BS11イレブン(Ch.11)","BS12 トゥエルビ(Ch.12)","BSキャンパスex"]
    for channel in filter_channels:
        ret = summary.find(channel)
        if ret != -1:
            return True
    return False

if __name__ == u'__main__':

    for rssurl in ['https://tv.so-net.ne.jp/rss/schedulesBySearch.action?stationPlatformId=0&condition.keyword=%E3%83%A9%E3%83%B4%E3%82%A7%E3%83%AB&submit=%E6%A4%9C%E7%B4%A2&stationAreaId=23&submit.x=&submit.y=']:
        sys.stderr.write(u'[info] rss=%s\n' % rssurl) 
        d = feedparser.parse(rssurl)

        # sys.stderr.write("%s\n" % d)
        for entry in d['entries']:
            if not check_channel(entry.summary):
                sys.stderr.write("[warn] skipping %s (%s)\n" % (entry.title, entry.summary))
                continue
            for item in entry.items():
                sys.stderr.write(u'[info] %s=%s\n' % (item[0], item[1]))
            # if entry.link not in checked_previously:
            #     sys.stderr.write(u"%s\n" % (entry.title, entry.link))
        
