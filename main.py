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
import gspread
import oauth2client.service_account


def str_abbreviate(str_in):
    len_str_in = len(str_in)
    if len_str_in > 128*2+10:
        return str_in[0:128] + " ... " + str_in[-128:]
    else:
        return str_in

def pg_execute(pg_cur, query, embedparam=[], param=[], show=True):
    param_str = str_abbreviate("%s" % param)
    if show:
        sys.stderr.write(u'[info] postgres: %s embedparam=%s param=%s\n' % (query, embedparam, param_str))
    else:
        sys.stderr.write(u'[info] postgres: %s embedparam=%s param=(hidden)\n' % (query, embedparam))
        # sys.stderr.write(u'[info] postgres: %s\n' % (query))
    return pg_cur.execute(query % embedparam, param)

def pg_init_json(pg_cur, table_name, key_name, show=True):
    pg_result = pg_execute(pg_cur, u"select 1 from pg_tables where schemaname='public' and tablename=%%s ;", embedparam=[], param=[table_name], show=show)
    pg_result = pg_cur.fetchone()
    if pg_result is None:
        #sys.stderr.write(u'[info] creating table\n')
        pg_execute(pg_cur, u"create table %s (key text unique, value text);", embedparam=table_name, show=show)
    elif 1 != pg_result[0] :
        raise Exception(u"exception")

    pg_execute(pg_cur, u'select value from %s where key=%%s;', embedparam=table_name, param=[key_name], show=show)
    pg_result = pg_cur.fetchone()
    
    if pg_result is None:
        pg_execute(pg_cur, u'insert into %s VALUES (%%s, %%s);', embedparam=table_name, param=[key_name, u"{}"], show=show)
        pg_data = {}
    else:
        if show:
            sys.stderr.write(u'[info] data=%s\n' % str_abbreviate(pg_result[0]))
        pg_data = json.loads(pg_result[0])
    return pg_data

def pg_update_json(pg_cur, table_name, key_name, pg_data):
    return pg_execute(pg_cur, u'update %s set value = %%s where key = %%s;', embedparam=table_name, param=[json.dumps(pg_data, ensure_ascii=False), key_name], show=show)

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

def get_cells_from_sheet(worksheet):
    return worksheet.range(gspread.utils.rowcol_to_a1(1, 1)+":"+gspread.utils.rowcol_to_a1(worksheet.row_count, worksheet.col_count))

def get_items_from_sheet(worksheet):
    # return [cell.value for cell in get_cells_from_sheet(worksheet)]
    items = []
    for cell in get_cells_from_sheet(worksheet):
        if cell.value:
            items.append(cell.value)
    return items

def update_sheet(worksheet, data):
    # worksheet.resize(1)
    len_data = len(data)
    sys.stderr.write("[info] update_sheet len_data=%s\n" % len(data))
    worksheet.clear()
    worksheet.resize(rows=len_data)
    cells = get_cells_from_sheet(worksheet)
    for i in range(len_data):
        cells[i].value = data[i]
    worksheet.update_cells(cells)

def check_match(html, node_text, keyword_list):
    detail_node_result = html.xpath('//*[*[text() = "%s"]]' % node_text)
    if len(detail_node_result) == 1:
        detail_node = detail_node_result[0]
        detail_node_html = lxml.etree.tostring(detail_node, encoding='utf-8').decode('utf-8')
        if check_text_match(detail_node_html, keyword_list):
            return True
    return False


# f_devnull = None
if __name__ == u'__main__':

    # f_devnulll = fopen(os.devnull, "w")
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7',
        'referer': 'https://tv.so-net.ne.jp/',
    }
    pg_url = os.environ[u'DATABASE_URL']
    table_name = u'generic_text_data'

    google_key_key_name = u'check_tv_google_key'
    sg_username = os.environ["SENDGRID_USERNAME"]
    sg_recipient = os.environ["SENDGRID_RECIPIENT"]
    sg_apikey = os.environ["SENDGRID_APIKEY"]
    pg_conn = psycopg2.connect(pg_url)
    pg_cur = pg_conn.cursor()

    google_key_json = pg_init_json(pg_cur, table_name, google_key_key_name, show=False)
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_dict(google_key_json, scope)
    gc = gspread.authorize(credentials)

    google_spreadsheet_key = os.environ[u'SPREADSHEET_KEY']
    worksheets = gc.open_by_key(google_spreadsheet_key).worksheets()
    checked_previously = []
    for worksheet in worksheets:
        if "check-tv-checked-previously"==worksheet.title:
            worksheet_checked_previously = worksheet
            checked_previously = get_items_from_sheet(worksheet)
            # sys.stderr.write("[info] checked_previously=%s\n" % checked_previously)
        elif "check-tv-keyword"==worksheet.title:
            worksheet_keyword = worksheet
            keyword_list = get_items_from_sheet(worksheet)
            sys.stderr.write("[info] keyword_list=%s\n" % keyword_list)
        elif "check-tv-filter-title"==worksheet.title:
            worksheet_filter_title = worksheet
            filter_title_list = get_items_from_sheet(worksheet)
            sys.stderr.write("[info] filter_title_list=%s\n" % filter_title_list)
        elif "check-tv-filter-channel"==worksheet.title:
            worksheet_filter_channel = worksheet
            filter_channel_list = get_items_from_sheet(worksheet)
            sys.stderr.write("[info] filter_channel_list=%s\n" % filter_channel_list)

    # check_tv_data = pg_init_json(pg_cur, table_name, key_name)
    # checked_previously = check_tv_data.get(u"checked_previously")
    # if checked_previously is None:
    #     checked_previously = []
    checked_thistime = []

    # keyword_list = check_tv_data.get(u"keyword_list")
    # if keyword_list is None:
    #     keyword_list = []

    # filter_channel_list = check_tv_data.get(u"filter_channel_list")
    # if filter_channel_list is None:
    #     filter_channel_list = []

    # filter_title_list = check_tv_data.get(u"filter_title_list")
    # if filter_title_list is None:
    #     filter_title_list = []

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
            #sys.stderr.write("[html]\n%s\n" % result.text)
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
    # check_tv_data[u'checked_previously'] = checked_thistime
    # pg_update_json(pg_cur, table_name, key_name, check_tv_data)
    
    pg_cur.close()

    if len(messages)>0:
        message_str = "<br />\n".join(messages)
        sys.stderr.write(u"[info] mailing via sendgrid\n")
        sg_client = sendgrid.SendGridAPIClient(sg_apikey)
        sg_from = sendgrid.Email(name="Check TV Programs", email=sg_username)
        message = sendgrid.Mail(from_email=sg_from, to_emails=[sg_recipient], subject=u"Update of TV Programs", html_content=message_str)
        message.reply_to = sg_recipient
        sg_client.send(message)
    
    update_sheet(worksheet_checked_previously, checked_thistime)
    pg_conn.commit()
    pg_conn.close()
    # f_devnulll.close()