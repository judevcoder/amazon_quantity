# -*- coding: utf-8 -*-
import logging
import datetime

import MySQLdb
from scrapy.conf import settings

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class QuantityPipeline(object):
    conn = MySQLdb.connect(host=settings.get('DB_HOST'), user=settings.get('DB_USER'), port=settings.get('DB_PORT'),
                           passwd=settings.get('DB_PASSWORD'), db=settings.get('DB_BASE'), charset='utf8')
    cur = conn.cursor()
    cur.execute('SET names UTF8')
    update_for_duplicate = settings.get('UPDATE_FOR_DUPLICATE')
    table = 'products'
    quantities = {}

    def open_spider(self, spider):
        query = "SELECT asin from queue WHERE status = 1"
        self.cur.execute(query)
        for row in self.cur.fetchall():
            spider.asins_start.append(row[0])

        logger.info('ASINs in db: %s', len(spider.asins_start))

        query = "SELECT proxy from proxies WHERE status = 1"
        self.cur.execute(query)
        spider.proxies = [row[0] for row in self.cur.fetchall()]

        query = "SELECT p1.asin, p1.seller, p1.quantity FROM products p1 LEFT JOIN products p2 " \
                "ON (p1.asin = p2.asin AND p1.id < p2.id) JOIN queue q ON (p1.asin = q.asin) " \
                "WHERE p2.id IS NULL and q.status = 1"
        # print query
        self.cur.execute(query)
        for r in self.cur.fetchall():
            # self.quantities[r[0]] = self.quantities.get(r[0], {})
            # self.quantities[r[0]][r[1]] = r[2]

            (lambda x: x[r[0]].update({r[1]: r[2]}) if r[0] in x else x.update({r[0]: {r[1]: r[2]}}))(self.quantities)

        logger.info('Quantities: %s', self.quantities)

    def process_item(self, item, spider):
        try:
            self._process_item(item, spider)
        except Exception:
            logger.exception(item)
        return item

    def _process_item(self, item, spider):
        if item['asin'] == item['source_asin']:
            query = "UPDATE queue SET status = 0 WHERE asin = '{}'".format(item['asin'])
            self.cur.execute(query)
            self.conn.commit()

        last_quantity = self.quantities.get(item['asin'], {item['seller']: 0}).get(item['seller'], 0) # АТАТА
        logger.debug('Last quantity: %s', last_quantity)
        # logging.debug([last_quantity, item['quantity']])
        if int(item['quantity']) < last_quantity:
            item['sold'] = last_quantity - int(item['quantity'])

        item['updated'] = datetime.datetime.now()
        keys = u'`, `'.join(item.keys())
        values = u"','".join([unicode(val).replace("'", "''").replace('\\', '\\\\') for val in item.values()])
        if self.update_for_duplicate:
            update = ', '.join([u"`{}` = '{}'".format(
                key, unicode(item[key]).replace("'", "''").replace('\\', '\\\\')) for key in item])
            query = u"INSERT INTO {} (`{}`) VALUES ('{}') ON DUPLICATE KEY UPDATE {}".format(
                self.table, keys, values, update)
        else:
            query = u"INSERT INTO {} (`{}`) VALUES ('{}')".format(self.table, keys, values)
        try:
            self.cur.execute(query)
            self.conn.commit()
        except MySQLdb.Error as e:
            logger.error(e)
            logger.error(u'BAD QUERY: {}'.format(query))
            raise
        return item


class KeywordPipeline(object):
    conn = MySQLdb.connect(host=settings.get('DB_HOST'), user=settings.get('DB_USER'), port=settings.get('DB_PORT'),
                           passwd=settings.get('DB_PASSWORD'), db=settings.get('DB_BASE'), charset='utf8')
    cur = conn.cursor()
    cur.execute('SET names UTF8')
    table = settings.get('DB_TABLE')
    update_for_duplicate = settings.get('UPDATE_FOR_DUPLICATE')
    table = 'keyword_products'

    def open_spider(self, spider):
        query = "SELECT asin, keyword from keyword_queue WHERE status = 1"
        self.cur.execute(query)
        for row in self.cur.fetchall():
            spider.keywords.append([row[0], row[1]])

        logger.info('%s keywords', len(spider.keywords))

        query = "SELECT proxy from proxies WHERE status = 1"
        self.cur.execute(query)
        spider.proxies = [row[0] for row in self.cur.fetchall()]

    def process_item(self, item, spider):
        query = "UPDATE keyword_queue SET status = 0 WHERE asin = '{}'".format(item['asin'])
        self.cur.execute(query)
        self.conn.commit()

        item['updated'] = datetime.datetime.now()
        keys = u'`, `'.join(item.keys())
        values = u"','".join([unicode(val).replace("'", "''").replace('\\', '\\\\') for val in item.values()])
        if self.update_for_duplicate:
            update = ', '.join([u"`{}` = '{}'".format(
                key, unicode(item[key]).replace("'", "''").replace('\\', '\\\\')) for key in item])
            query = u"INSERT INTO {} (`{}`) VALUES ('{}') ON DUPLICATE KEY UPDATE {}".format(
                self.table, keys, values, update)
        else:
            query = u"INSERT INTO {} (`{}`) VALUES ('{}')".format(self.table, keys, values)
        try:
            self.cur.execute(query)
            self.conn.commit()
        except MySQLdb.Error as e:
            logger.error(e)
            logger.error(u'BAD QUERY: {}'.format(query))
        return item
