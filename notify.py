#!/usr/bin/python
#  -*- coding: utf-8 -*-
""" Notify subscribers with matching procurement declarations for
Taiwan government e-procurement website"""

import json
import logging
import time
import datetime as dt
import smtplib
import mysql.connector
from optparse import OptionParser
from smtplib import SMTPException
from email.message import EmailMessage
from mysql.connector import errorcode

__author__ = "Yu-chun Huang"
__version__ = "1.0.0b"

_ERRCODE_DATE = 2
_ERRCODE_FILE = 3

_EMAIL_SENDER = 'devops@chtsecurity.com'

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    p = OptionParser()

    # Query arguments
    p.add_option('-s', '--date_start', action='store',
                 dest='date_start', type='string', default=time.strftime('%Y%m%d'))
    p.add_option('-n', '--notify_config', action='store',
                 dest='notify_config', type='string', default='')
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

    # SMTP arguments
    p.add_option('-j', '--m_user', action='store',
                 dest='m_user', type='string', default='')
    p.add_option('-k', '--m_password', action='store',
                 dest='m_password', type='string', default='')
    p.add_option('-l', '--m_host', action='store',
                 dest='m_host', type='string', default='')
    return p.parse_args()


def gen_select_sql(table, start_date, org_names=None, subjects=None):
    sql_str = u'SELECT * FROM {} WHERE (declare_date >= {}) '.format(table, start_date.strftime('%Y-%m-%d'))

    sql_keyword = ''
    if org_names is not None and len(org_names) > 0:
        sql_keyword += u' OR '.join([u'org_name LIKE \'%{}%\''.format(w) for w in org_names])

    if subjects is not None and len(subjects) > 0:
        if sql_keyword:
            sql_keyword += ' OR '
        sql_keyword += u' OR '.join([u'subject LIKE \'%{}%\''.format(w) for w in subjects])

    if sql_keyword:
        sql_str += 'AND (' + sql_keyword + ')'
    logger.debug(sql_str)
    return sql_str


def send_mail(sender, recipients, subject, content, server, username, pwd):
    msg = EmailMessage()
    msg.set_content(content)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    # Send the message via our own SMTP server.
    try:
        logger.info('Sending notification Email.')
        s = smtplib.SMTP(server)
        s.login(username, pwd)
        s.send_message(msg)
        logger.info('Notification Email sent.')
    except SMTPException as e:
        logger.error("Error: unable to send email")
        logger.error(e)


if __name__ == '__main__':
    options, remainder = parse_args()

    # Query arguments
    try:
        start = dt.datetime.strptime(options.date_start.strip(), '%Y%m%d').date()
    except ValueError:
        logger.error('Invalid start date.')
        quit(_ERRCODE_DATE)

    # Database arguments
    user = options.user.strip()
    password = options.password.strip()
    host = options.host.strip()
    port = options.port.strip()
    database = options.database.strip()
    if not (user and password and host and port and database):
        logger.error('Database connection information is incomplete.')
        quit()

    connection_info = {'user': user,
                       'password': password,
                       'host': host,
                       'port': port,
                       'database': database
                       }

    # SMTP arguments
    m_user = options.m_user.strip()
    m_password = options.m_password.strip()
    m_host = options.m_host.strip()
    if not (m_user and m_password and m_host):
        logger.error('SMTP information is incomplete.')
        quit()

    # Log query arguments
    logger.info('Start date: %s', start.strftime('%Y-%m-%d'))

    try:
        f = open(options.notify_config.strip(), encoding='UTF-8')
        config = json.load(f)
    except IOError:
        logger.error('Unable to open notification configuration file.')
        quit(_ERRCODE_FILE)

    # Start query
    try:
        content_template = u'標案案號：{}\n機關名稱：{}\n標案名稱：{}\n招標方式：{}\n採購性質：{}\n公告日期：{}\n截止投標日期：{}\n預算金額：{}標案網址：{}\n\n'
        content = ''

        cnx = mysql.connector.connect(**connection_info)
        cnx.autocommit = True
        for subscriber in config:
            receivers = subscriber['email']
            org_names = subscriber['keyword_org'] if 'keyword_org' in subscriber else None
            subjects = subscriber['keyword_subject'] if 'keyword_subject' in subscriber else None
            query = gen_select_sql('declaration_notify', start, org_names, subjects)

            cursor = cnx.cursor(buffered=True, dictionary=True)
            cursor.execute(query)

            for row in cursor:
                content += content_template.format(row['id'],
                                                   row['org_name'],
                                                   row['subject'],
                                                   row['method'],
                                                   row['category'],
                                                   row['declare_date'],
                                                   row['deadline'],
                                                   row['budget'],
                                                   row['url'])
            cursor.close()
            send_mail(m_user, receivers, '政府採購網標案公告通知', content, m_host, m_user, m_password)
        logger.info(content)
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
    else:
        cnx.close()