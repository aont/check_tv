#!/usr/bin/env python3

import feedparser
import sys
import datetime
import psycopg2
import json
import os
import requests
import urllib.parse
import sendgrid
import re
import lxml
import lxml.html
import time

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
    return pg_execute(pg_cur, u'update %s set value = %%s where key = %%s;' % table_name, [json.dumps(pg_data, ensure_ascii=False), key_name])

def keyword2rss(keyword_list):
    rss_list = []
    base_url = u"https://tv.so-net.ne.jp/rss/schedulesBySearch.action?condition.genres%5B0%5D.parentId=-1&condition.genres%5B0%5D.childId=-1&submit=%E6%A4%9C%E7%B4%A2&stationAreaId=23&submit.x=&submit.y="
    for keyword in keyword_list:
        rss_list.append(base_url + "&condition.keyword=%s&stationPlatformId=%s" % (urllib.parse.quote(keyword, safe=''), 0))
    return rss_list

def filter_channel(summary, filter_channels):
    for channel in filter_channels:
        ret = summary.find(channel)
        if ret != -1:
            return True
    return False

def filter_title(title, filter_title_list):
    if title in filter_title_list:
        return False
    return True

def check_text_match(txt, keyword_list):
    for keyword in keyword_list:
        if -1 != txt.find(keyword):
            sys.stderr.write("[info] %s matches\n" % keyword)
            return True
    return False


def check_match(html, node_text, keyword_list):
    detail_node_result = html.xpath('//*[*[text() = "%s"]]' % node_text)
    if len(detail_node_result) == 1:
        detail_node = detail_node_result[0]
        detail_node_html = lxml.etree.tostring(detail_node, encoding='utf-8').decode('utf-8')
        if check_text_match(detail_node_html, keyword_list):
            return True
    return False


if __name__ == u'__main__':

    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7',
        'referer': 'https://tv.so-net.ne.jp/',
    }
    pg_url = os.environ[u'DATABASE_URL']
    table_name = u'generic_text_data'
    key_name = u'check_tv'
    sg_username = os.environ["SENDGRID_USERNAME"]
    sg_recipient = os.environ["SENDGRID_RECIPIENT"]
    sg_apikey = os.environ["SENDGRID_APIKEY"]
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

    filter_channel_list = check_tv_data.get(u"filter_channel_list")
    if filter_channel_list is None:
        filter_channel_list = []

    filter_title_list = check_tv_data.get(u"filter_title_list")
    if filter_title_list is None:
        filter_title_list = []

    url_pat=re.compile(u'https://tv.so-net.ne.jp/schedule/(\\d+)\\.action\\?from=rss')
    messages = []
    sess = requests.session()
    for rssurl in keyword2rss(keyword_list):
        sys.stderr.write(u'[info] rss=%s\n' % rssurl) 
        d = feedparser.parse(rssurl)

        for entry in d['entries']:

            url_match = url_pat.match(entry.link)
            url_num = url_match.group(1)

            if url_match is None:
                raise Exception(u'unexpected')
            if url_num in checked_thistime:
                sys.stderr.write("[info] skipping %s (duplicate)\n" % (entry.title))
                continue

            if url_num in checked_previously:
                sys.stderr.write("[info] skipping %s (checked previously)\n" % (entry.title))
                checked_thistime.append(url_num)
                continue
            elif not filter_channel(entry.summary, filter_channel_list):
                sys.stderr.write("[info] skipping  %s (%s) (channnel is filtered)\n" % (entry.title, entry.summary))
                checked_thistime.append(url_num)
                continue
            elif not filter_title(entry.title, filter_title_list):
                sys.stderr.write("[info] skipping  %s (program is filtered)\n" % (entry.title))
                checked_thistime.append(url_num)
                continue


            sys.stderr.write("[url] %s\n" % entry.link)
            result = sess.get(entry.link, headers=headers)
            # time.sleep(1)
            if result.status_code != requests.status_codes.codes.get("ok"):
                raise Exception('unexpected')
            sys.stderr.write("[html]\n%s\n" % result.text)
            html = lxml.html.fromstring(result.text, base_url=entry.link)

            if not check_text_match(entry.title, keyword_list):
                if not check_match(html, "番組概要", keyword_list):
                    if not check_match(html, "人名リンク", keyword_list):
                        if not check_match(html, "番組詳細", keyword_list):
                            sys.stderr.write("[info] skipping %s (no matching keyword)\n" % entry.link)
                            # checked_thistime.append(url_num)
                            # input("")
                            continue
            
            checked_thistime.append(url_num)
            mes = u"<a href=\"%s\">%s</a>" % (entry.link, entry.title)
            messages.append(mes)

    sess.close()
    check_tv_data[u'checked_previously'] = checked_thistime
    pg_update_json(pg_cur, table_name, key_name, check_tv_data)
    
    pg_cur.close()

    if len(messages)>0:
        message_str = "<br />\n".join(messages)
        sys.stderr.write(u"[info] mailing via sendgrid\n")
        sg_client = sendgrid.SendGridAPIClient(sg_apikey)
        sg_from = sendgrid.Email(name="Check TV Programs", email=sg_username)
        message = sendgrid.Mail(from_email=sg_from, to_emails=[sg_recipient], subject=u"Update of TV Programs", html_content=message_str)
        message.reply_to = sg_recipient
        sg_client.send(message)
    
    pg_conn.commit()
    pg_conn.close()