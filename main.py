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
import html

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
    return pg_cur.execute(query % embedparam, param)

def pg_init_json(pg_cur, table_name, key_name, show=True):
    pg_result = pg_execute(pg_cur, u"select 1 from pg_tables where schemaname='public' and tablename=%%s ;", embedparam=[], param=[table_name], show=show)
    pg_result = pg_cur.fetchone()
    if pg_result is None:
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

def keyword2rss(keyword):
    base_url = u"https://tv.so-net.ne.jp/rss/schedulesBySearch.action?condition.genres%5B0%5D.parentId=-1&condition.genres%5B0%5D.childId=-1&submit=%E6%A4%9C%E7%B4%A2&stationAreaId=23&submit.x=&submit.y="
    return base_url + "&condition.keyword=%s&stationPlatformId=%s" % (urllib.parse.quote(keyword, safe=''), 0)

# def keyword2rss(keyword_list):
#     base_url = u"https://tv.so-net.ne.jp/rss/schedulesBySearch.action?condition.genres%5B0%5D.parentId=-1&condition.genres%5B0%5D.childId=-1&submit=%E6%A4%9C%E7%B4%A2&stationAreaId=23&submit.x=&submit.y="
#     for keyword in keyword_list:
#         yield base_url + "&condition.keyword=%s&stationPlatformId=%s" % (urllib.parse.quote(keyword, safe=''), 0)

def check_filter_channel_list(summary, filter_channels):
    for channel in filter_channels:
        ret = summary.find(channel)
        if ret != -1:
            return True
    return False

# def check_filter_title(title, filter_title):
#     if filter_title in title:
#         return False
#     return True

def check_filter_title_list(title, filter_title_list):
    for filter_title in filter_title_list:
        if filter_title in title:
            return False
    return True


def get_section(html, node_text):
    detail_node_result = html.xpath('//*[*[text() = "%s"]]' % node_text)
    if len(detail_node_result) == 1:
        detail_node = detail_node_result[0]
        detail_node_html = lxml.etree.tostring(detail_node, encoding='utf-8').decode('utf-8')
        return detail_node_html
    else:
        sys.stderr.write("[warn] section %s not found\n" % node_text)
        return ""

def get_cells_from_sheet(worksheet):
    return worksheet.range(gspread.utils.rowcol_to_a1(1, 1)+":"+gspread.utils.rowcol_to_a1(worksheet.row_count, worksheet.col_count))

def get_items_from_sheet(worksheet):
    for cell in get_cells_from_sheet(worksheet):
        if cell.value:
            yield cell.value

def get_db_from_sheet(worksheet):
    row_count = worksheet.row_count
    col_count = worksheet.col_count

    cells = worksheet.range(gspread.utils.rowcol_to_a1(1, 1)+":"+gspread.utils.rowcol_to_a1(row_count, col_count))
    mapping = {}
    for col_num in range(col_count):
        cell = cells[0*col_count+col_num]
        if cell.value:
            mapping[cell.value] = col_num

    for row_num in range(1, row_count):
        item = {}
        has_item = False
        for key, col_num in mapping.items():
            cell = cells[row_num*col_count+col_num]
            if cell.value:
                item[key] = cell.value
                has_item = True
        if has_item:
            yield item

# not tested yet
def update_db_from_sheet(worksheet, data):
    len_data = len(data)
    # resize_worksheet(worksheet, len_data)
    worksheet.resize(rows=len_data)
    row_count = len_data
    col_count = worksheet.col_count
    cells = worksheet.range(gspread.utils.rowcol_to_a1(1, 1)+":"+gspread.utils.rowcol_to_a1(row_count, col_count))

    mapping = {}
    for col_num in range(col_count):
        cell = cells[0*col_count+col_num]
        if cell.value:
            mapping[cell.value] = col_num

    for row_num in range(1, row_count):
        cells[row_num*col_count+col_num].value = None

    for data_num in range(len_data):
        row_num = data_num + 1
        data_i = data[data_num]
        for key, col_num in mapping.items():
            cell = cells[row_num*col_count+col_num]
            cell.value = data_i[key]

# def resize_worksheet(worksheet, rows_):
#     rows = rows_ + 30
#     try_num = 0
#     max_try = 5
#     while True:
#         worksheet.resize(rows=rows)
#         try_num += 1
#         if worksheet.row_count != rows:
#             if try_num < max_try:
#                 sys.stderr.write("[info] retry resizing %s <- %s\n" % (rows, worksheet.row_count))
#                 time.sleep(10)
#                 continue
#             else:
#                 raise Exception("update_sheet: resize failed %s <- %s" % (rows, worksheet.row_count))
#         else:
#             break

def update_sheet(worksheet, data):
    len_data = len(data)
    sys.stderr.write("[info] update_sheet len_data=%s\n" % len(data))
    worksheet.resize(rows=len_data)
    # resize_worksheet(worksheet, len_data+10)
    cells = worksheet.range(gspread.utils.rowcol_to_a1(1, 1)+":"+gspread.utils.rowcol_to_a1(len_data, worksheet.col_count))
    # cells = get_cells_from_sheet(worksheet)
    if len(cells) < len_data:
        raise Exception("unexpected")
    for cell in cells:
        cell.value = None
    for i in range(len_data):
        cells[i].value = data[i]
    worksheet.update_cells(cells)


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

    google_key_key_name = u'check_tv_google_key'
    sg_username = os.environ["SENDGRID_USERNAME"]
    sg_recipient = os.environ["SENDGRID_RECIPIENT"]
    sg_apikey = os.environ["SENDGRID_APIKEY"]
    pg_conn = psycopg2.connect(pg_url)
    pg_cur = pg_conn.cursor()
    google_key_json = pg_init_json(pg_cur, table_name, google_key_key_name, show=False)
    pg_cur.close()
    pg_conn.commit()
    pg_conn.close()

    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_dict(google_key_json, scope)
    gc = gspread.authorize(credentials)

    google_spreadsheet_key = os.environ[u'SPREADSHEET_KEY']
    worksheets = gc.open_by_key(google_spreadsheet_key).worksheets()
    checked_previously = []
    for worksheet in worksheets:
        if "check-tv-checked-previously"==worksheet.title:
            worksheet_checked_previously = worksheet
            checked_previously = list(get_items_from_sheet(worksheet))
        elif "check-tv-queries"==worksheet.title:
            worksheet_queries = worksheet
            query_db = list(get_db_from_sheet(worksheet))
        elif "check-tv-filter-title"==worksheet.title:
            worksheet_filter_title = worksheet
            filter_title_list = list(get_items_from_sheet(worksheet))
            sys.stderr.write("[info] filter_title_list=%s\n" % filter_title_list)
        elif "check-tv-filter-channel"==worksheet.title:
            worksheet_filter_channel = worksheet
            filter_channel_list = list(get_items_from_sheet(worksheet))
            sys.stderr.write("[info] filter_channel_list=%s\n" % filter_channel_list)

    checked_thistime = []

    url_pat=re.compile(u'https://tv.so-net.ne.jp/schedule/(\\d+)\\.action\\?from=rss')
    messages = []
    sess = requests.session()

    for query in query_db:
        keyword = query.get("keyword")
        if keyword is None:
            raise Exception("keyword is None")
        title_exclude = query.get("title-exclude")
        rssurl = keyword2rss(keyword)
        d = feedparser.parse(rssurl)

        datetime_now = datetime.datetime.now()
        delta_days_th = 6
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

            if title_exclude and (title_exclude in entry.title):
                sys.stderr.write("[info] skipping  %s (title is filtered)\n" % (entry.title))
                continue

            if not check_filter_channel_list(entry.summary, filter_channel_list):
                sys.stderr.write("[info] skipping  %s (%s) (channnel is filtered)\n" % (entry.title, entry.summary))
                continue

            if not check_filter_title_list(entry.title, filter_title_list):
                sys.stderr.write("[info] skipping  %s (title is filtered)\n" % (entry.title))
                continue

            date_datetime = datetime.datetime.strptime(entry.date, "%Y-%m-%dT%H:%M+09:00")
            delta_time = date_datetime - datetime_now

            if delta_time.days > delta_days_th:
                sys.stderr.write("[info] skipping %s (delta_days=%s > %s)\n" % (entry.title, delta_time.days, delta_days_th))
                continue

            sys.stderr.write("[url] %s\n" % entry.link)
            max_retry = 3
            sleep_dur = 5
            for t in range(max_retry):
                try:
                    result = sess.get(entry.link, headers=headers)
                    result.raise_for_status()
                    break
                except Exception as e:
                    if t>max_retry-1:
                        raise e
                    else:
                        sys.stderr.write("[warn] sleep and retry\n")
                        time.sleep(sleep_dur)
                        continue

            program_html = lxml.html.fromstring(result.text, base_url=entry.link)

            if not keyword in entry.title:
                if not keyword in get_section(program_html, "番組概要"):
                    if not keyword in get_section(program_html, "人名リンク"):
                        if not keyword in get_section(program_html, "番組詳細"):
                            sys.stderr.write("[info] skipping %s (no matching keyword)\n" % entry.link)
                            continue

            checked_thistime.append(url_num)
            mes = u"<a href=\"%s\">%s</a> （%s）" % (entry.link, html.escape(entry.title), keyword)
            messages.append(mes)

    sess.close()

    if len(messages)>0:
        message_str = "<br />\n".join(messages)
        sys.stderr.write(u"[info] mailing via sendgrid\n")
        sg_client = sendgrid.SendGridAPIClient(sg_apikey)
        sg_from = sendgrid.Email(name="Check TV Programs", email=sg_username)
        message = sendgrid.Mail(from_email=sg_from, to_emails=[sg_recipient], subject=u"Update of TV Programs", html_content=message_str)
        message.reply_to = sg_recipient
        sg_client.send(message)

    update_sheet(worksheet_checked_previously, checked_thistime)
