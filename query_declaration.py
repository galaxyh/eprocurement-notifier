#!/usr/bin/python
#  -*- coding: utf-8 -*-
""" Queryer for Taiwan government e-procurement website
Modified from the source code provided by https://github.com/ywchiu/pythonetl"""

import requests
import logging
import time
import datetime as dt
import mysql.connector
import re
from urllib import parse
from optparse import OptionParser
from bs4 import BeautifulSoup
from math import ceil
from datetime import datetime, date
from mysql.connector import errorcode

__author__ = "Yu-chun Huang"
__version__ = "1.0.0b"

_ERRCODE_DATE = 2

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    p = OptionParser()

    # Query arguments
    p.add_option('-s', '--date_start', action='store',
                 dest='date_start', type='string', default=time.strftime('%Y%m%d'))
    p.add_option('-e', '--date_end', action='store',
                 dest='date_end', type='string', default=time.strftime('%Y%m%d'))
    p.add_option('-o', '--org_name', action='store',
                 dest='org_name', type='string', default='')
    p.add_option('-j', '--procurement_subject', action='store',
                 dest='procurement_subject', type='string', default='')
    p.add_option('-f', '--err_filename', action='store',
                 dest='err_filename', type='string', default='error')

    # Database arguments
    p.add_option('-u', '--user', action='store',
                 dest='user', type='string', default='')
    p.add_option('-p', '--password', action='store',
                 dest='password', type='string', default='')
    p.add_option('-i', '--host', action='store',
                 dest='host', type='string', default='')
    p.add_option('-b', '--database', action='store',
                 dest='database', type='string', default='')
    p.add_option('-t', '--port', action='store',
                 dest='port', type='string', default='3306')
    return p.parse_args()


def ad2roc(date, separator=''):
    roc = str(date.year - 1911)
    roc += separator + '{0:02d}'.format(date.month)
    roc += separator + '{0:02d}'.format(date.day)
    return roc


def roc2ad(element):
    m = re.match(r'(?P<date>\d+/\d+/\d+)(\s+)*(?P<time>\d+:\d+)?', element.strip())
    if m is not None:
        d = [int(n) for n in m.group('date').split('/')]
        t = [int(n) for n in m.group('time').split(':')] if m.group('time') is not None else None
        if d[0] != '':
            if t is not None:
                return datetime(d[0] + 1911, d[1], d[2], hour=t[0], minute=t[1])
            else:
                return date(d[0] + 1911, d[1], d[2])
    return None


trantab = str.maketrans(
    {'\'': '\\\'',
     '\"': '\\\"',
     '\b': '\\b',
     '\n': '\\n',
     '\r': '\\r',
     '\t': '\\t',
     '\\': '\\\\', })


def gen_insert_sql(table, data_dict):
    sql_template = u'INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}'
    columns = ''
    values = ''
    dup_update = ''

    for k, v in data_dict.items():
        if v is not None:
            if values != '':
                columns += ','
                values += ','
                dup_update += ','

            columns += k

            if isinstance(v, str):
                vstr = '\'' + v.translate(trantab) + '\''
            elif isinstance(v, bool):
                vstr = '1' if v else '0'
            elif isinstance(v, datetime) or isinstance(v, date):
                vstr = '\'' + str(v) + '\''
            else:
                vstr = str(v)

            values += vstr
            dup_update += k + '=' + vstr

    sql_str = sql_template.format(table, columns, values, dup_update)
    logger.debug(sql_str)
    return sql_str


if __name__ == '__main__':
    options, remainder = parse_args()

    # Query arguments
    date_range = ('', '')
    try:
        date_range = (dt.datetime.strptime(options.date_start.strip(), '%Y%m%d').date(),
                      dt.datetime.strptime(options.date_end.strip(), '%Y%m%d').date())
        if date_range[0] > date_range[1]:
            logger.error('Start date must be smaller than or equal to end date.')
            quit(_ERRCODE_DATE)
    except ValueError:
        logger.error('Invalid start/end date.')
        quit(_ERRCODE_DATE)

    org_name = options.org_name.strip()
    procurement_subject = options.procurement_subject.strip()

    # Database arguments
    user = options.user.strip()
    password = options.password.strip()
    host = options.host.strip()
    port = options.port.strip()
    database = options.database.strip()
    if user == '' or password == '' or host == '' or port == '' or database == '':
        logger.error('Database connection information is incomplete.')
        quit()

    connection_info = {'user': user,
                       'password': password,
                       'host': host,
                       'port': port,
                       'database': database
                       }

    # Log query arguments
    logger.info('Start date: %s, End date: %s, List filename: %s',
                date_range[0].strftime('%Y-%m-%d'), date_range[1].strftime('%Y-%m-%d'),
                options.err_filename.strip())
    logstr = ''
    if org_name != '':
        logstr = 'Organization name: %s', org_name
    if procurement_subject != '':
        logstr = 'Procurement subject: %s', procurement_subject
    if logstr != '':
        logger.info('Organization name: %s', org_name)

    # Limit maximum search date span to be within 3 months (consider Feb. can has only 28 days)
    max_span = 89
    totalDays = (date_range[1] - date_range[0]).days

    # Start query
    try:
        cnx = mysql.connector.connect(**connection_info)
        cnx.autocommit = False
        cur = cnx.cursor(buffered=True)
        cur.execute('SET NAMES utf8mb4')

        for i in range(0, int(totalDays / max_span) + 1):
            s_date = date_range[0] + dt.timedelta(days=i * (max_span - 1) + i)
            e_date = min(date_range[1], s_date + dt.timedelta(days=max_span - 1))

            logger.info('Searching for bids from %s to %s...',
                        s_date.strftime('%Y-%m-%d'), e_date.strftime('%Y-%m-%d'))

            # Search parameters
            payload = {'method': 'search',
                       'searchMethod': 'true',
                       'tenderUpdate': '',
                       'searchTarget': '',
                       'orgName': org_name,
                       'orgId': '',
                       'hid_1': '1',
                       'tenderName': procurement_subject,
                       'tenderId': '',
                       'tenderType': 'tenderDeclaration',
                       'tenderWay': '1,2,3,4,5,6,7,10,12',
                       'tenderDateRadio': 'on',
                       'tenderStartDateStr': ad2roc(s_date, '/'),
                       'tenderEndDateStr': ad2roc(e_date, '/'),
                       'tenderStartDate': ad2roc(s_date, '/'),
                       'tenderEndDate': ad2roc(e_date, '/'),
                       'isSpdt': 'N',
                       'proctrgCate': '',
                       'btnQuery': '查詢',
                       'hadUpdated': ''}

            try:
                rs = requests.session()
                user_post = rs.post('http://web.pcc.gov.tw/tps/pss/tender.do?'
                                    'searchMode=common&'
                                    'searchType=basic',
                                    data=payload)
                response_text = user_post.text.encode('utf8')

                soup = BeautifulSoup(response_text, 'lxml')
                rec_number_element = soup.find('span', {'class': 'T11b'})
                rec_number = int(rec_number_element.text)
                page_number = int(ceil(float(rec_number) / 100))

                logger.info('\tTotal number of bids: %d', rec_number)
            except Exception as e:
                logger.warning(e)
                with open(options.err_filename.strip() + '.query.log', 'a', encoding='utf-8') as err_file:
                    err_file.write(str(s_date) + '\t' + str(e_date) + '\n')
                continue

            page_format = 'http://web.pcc.gov.tw/tps/pss/tender.do?' \
                          'searchMode=common&' \
                          'searchType=basic&' \
                          'method=search&' \
                          'isSpdt=&' \
                          'pageIndex=%d'
            for page in range(1, page_number + 1):
                logger.info('\tRetrieving bid URLs... (%d / %d)', min(page * 100, rec_number), rec_number)

                try:
                    bid_list = rs.get(page_format % page)
                    bid_response = bid_list.text.encode('utf8')
                    bid_soup = BeautifulSoup(bid_response, 'lxml')
                    bid_table = bid_soup.find('div', {'id': 'print_area'})
                    bid_rows = bid_table.findAll('tr')[1:-1]
                    for bid_row in bid_rows:
                        link = [tag['href'] for tag in bid_row.findAll('a', {'href': True})][0]
                        link_href = parse.urljoin('http://web.pcc.gov.tw/tps/pss/tender.do?'
                                                  'searchMode=common&'
                                                  'searchType=basic',
                                                  link)
                        content = bid_row.findAll('td')
                        id = content[2].text.strip().split()[0]
                        budget_str = ''.join(content[8].text.strip().split())
                        data = {'id': id,
                                'org_name': ''.join(content[1].text.strip().split()),
                                'subject': ' '.join(content[2].text.strip().split()),
                                'method': ''.join(content[4].text.strip().split()),
                                'category': ''.join(content[5].text.strip().split()),
                                'declare_date': roc2ad(''.join(content[6].text.strip().split())),
                                'deadline': roc2ad(''.join(content[7].text.strip().split())),
                                'budget': None if not budget_str else int(budget_str),
                                'url': link_href}
                        cur.execute(gen_insert_sql('declaration_notify', data))
                except Exception as e:
                    logger.warning(e)
                    with open(options.err_filename.strip() + '.page.log', 'a', encoding='utf-8') as err_file:
                        err_file.write(page_format % page + '\n')
                    continue

                cnx.commit()
                time.sleep(1)  # Prevent from being treated as a DDOS attack
    except mysql.connector.Error as e:
        if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Something is wrong with your user name or password.")
        elif e.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("Database does not exist.")
        else:
            outstr = 'Fail to update database.\n\t{}'.format(e)
            logger.warning(outstr)
            with open(options.err_filename.strip() + '.load.err', 'a', encoding='utf-8') as err_file:
                err_file.write(outstr)
    except AttributeError as e:
        outstr = 'Corrupted content. Update skipped.\n\t{}'.format(e)
        logger.warning(outstr)
        with open(options.err_filename.strip() + 'load.err', 'a', encoding='utf-8') as err_file:
            err_file.write(outstr)
    else:
        cnx.close()

    logger.info('All done.')
