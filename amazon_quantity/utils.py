from contextlib import contextmanager
import logging

import MySQLdb
import lxml.html

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


def other_if_empty(first, other):
    """
    Generate xpath expression returns 'first' if it exists and 'other' if 'first' not exists
    PAY ATTENTION:  expression MUST starts from "/*//node" not from ".//node"
    https://stackoverflow.com/a/12370517/4249707 for realization details
    """
    return '{} | {}[not({})]'.format(first, other, first)


def get_etree(rs):
    etree = lxml.html.fromstring(rs.body)
    etree.make_links_absolute(rs.url)
    return etree


def clone_from_response(rq, rs=None, payload=None):
    if payload is None:
        payload = {}
    if rs:
        for k in 'proxy', 'cookiejar', 'item':
            try:
                rq.meta[k] = rs.meta[k]
            except KeyError:
                pass
        rq.meta.update({k: v for k, v in rs.meta.items() if k.startswith('_')})
    for k, v in payload.items():
        rq.meta['item'][k] = v
    return rq


def load_from_dict(loader, item):
    for k, v in item.items():
        if k.startswith('_'):
            continue
        loader.add_value(k, v)
    return loader.load_item()


def create_connection(settings):
    return MySQLdb.connect(
            host=settings.get('DB_HOST'),
            user=settings.get('DB_USER'),
            port=settings.get('DB_PORT'),
            passwd=settings.get('DB_PASSWORD'),
            db=settings.get('DB_BASE'),
            charset='utf8'
        )


def get_data(query, conn=None, settings=None):
    if conn is None:
        conn = create_connection(settings)
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(query)
    payload = cursor.fetchall()
    # server-side cursors https://kushaldas.in/posts/fetching-row-by-row-from-mysql-in-python.html
    if settings:
        conn.close()
    return payload