# -*- coding: utf-8 -*-
import collections
import logging
import pprint

from MySQLdb import OperationalError
from MySQLdb.constants.CR import SERVER_GONE_ERROR,  SERVER_LOST, CONNECTION_ERROR
from MySQLdb.cursors import DictCursor
from twisted.internet import defer
from twisted.enterprise import adbapi

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class MySQLPipeline(object):
    stats_name = 'mysql_pipeline'

    retries = 3
    close_on_error = True
    table = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):
        self.stats = crawler.stats
        self.settings = crawler.settings
        db_args = {
            'host': self.settings.get('DB_HOST'),
            'user': self.settings.get('DB_USER'),
            'port': self.settings.get('DB_PORT'),
            'passwd': self.settings.get('DB_PASSWORD'),
            'db': self.settings.get('DB_BASE'),
            'charset': 'utf8',
            # 'autocommit': True,
            'cursorclass': DictCursor,
            'cp_reconnect': True,
        }
        self.db = adbapi.ConnectionPool('MySQLdb', **db_args)
        self.update_for_duplicate = self.settings.get('UPDATE_FOR_DUPLICATE')

    def close_spider(self, spider):
        self.db.close()

    def _preprocess_item(self, item):
        """Do something interesting with item"""
        return item

    def _postprocess_item(self, *args):
        """Update query table etc."""
        pass

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        retries = self.retries
        status = -1
        while retries:
            try:
                item = self._preprocess_item(item)
                yield self.db.runInteraction(self._process_item, item)
            except OperationalError as e:
                if e[0] in (
                        SERVER_GONE_ERROR,
                        SERVER_LOST,
                        CONNECTION_ERROR
                ):
                    retries -= 1
                    logger.info('%s %s attempts to reconnect left', e, retries)
                    self.stats.inc_value('{}/reconnects'.format(self.stats_name))
                    continue
                logger.exception('%s', pprint.pformat(item))
                self.stats.inc_value('{}/errors'.format(self.stats_name))
            except Exception:
                logger.exception('%s', pprint.pformat(item))
                self.stats.inc_value('{}/errors'.format(self.stats_name))
            else:
                status = 0
            break
        else:
            if self.close_on_error:
                spider.crawler.engine.close_spider(spider, '{}_fatal_error'.format(self.stats_name))
        self._postprocess_item(item, status)
        yield item

    def _generate_sql(self, data):
        placeholders = lambda d: ', '.join(['%s'] * len(d))
        columns = lambda d: ', '.join(['`{}`'.format(k) for k in d])
        values = lambda d: [v for v in d.values()]
        on_duplicate_placeholders = lambda d: ', '.join(['`{}` = %s'.format(k) for k in d])
        insert_template = 'INSERT INTO `{}` ( {} ) VALUES ( {} )'
        upsert_template = 'INSERT INTO `{}` ( {} ) VALUES ( {} ) ON DUPLICATE KEY UPDATE {}'
        if self.update_for_duplicate:
            return (
                upsert_template.format(
                    self.table, columns(data),
                    placeholders(data), on_duplicate_placeholders(data)
                ),
                values(data) + values(data)
            )
        else:
            return (
                insert_template.format(self.table, columns(data), placeholders(data)),
                values(data)
            )

    def _process_item(self, tx, row):
        sql, data = self._generate_sql(row)
        try:
            tx.execute(sql, data)
        except Exception:
            logger.error("SQL: %s", sql)
            raise
        self.stats.inc_value('{}/saved'.format(self.stats_name))


class KeywordPipeline(MySQLPipeline):
    def __init__(self, *args, **kwargs):
        super(KeywordPipeline, self).__init__(*args, **kwargs)
        self.table = 'keyword_products'

    @defer.inlineCallbacks
    def _postprocess_item(self, data, status):
        if status == -1:  # It's bad choice. You can't track errors
            status = 1
        yield self.db.runOperation(
            "UPDATE `keyword_queue` SET `status` = %s WHERE `asin` = %s AND `keyword` = %s",
            (status, data['asin'], data['keyword'])
        )


class QuantifyPipeline(MySQLPipeline):
    def __init__(self, *args, **kwargs):
        super(QuantifyPipeline, self).__init__(*args, **kwargs)
        self.table = 'products'
        self.quantities = collections.defaultdict(dict)
        self._init_quantities()

    @defer.inlineCallbacks
    def _init_quantities(self):
        query = "SELECT p1.asin, p1.seller, p1.quantity FROM products p1 LEFT JOIN products p2 " \
                "ON (p1.asin = p2.asin AND p1.id < p2.id) JOIN queue q ON (p1.asin = q.asin) " \
                "WHERE p2.id IS NULL AND q.status = 1"  # TODO Something strange here
        result = yield self.db.runQuery(query)
        for r in result:
            self.quantities[r['asin']][r['seller']] = r['quantity']
        logger.info('Quantities: %s', self.quantities)

    @defer.inlineCallbacks
    def _postprocess_item(self, data, status):
        if status == -1:  # It's bad choice. You can't track errors
            status = 1
        yield self.db.runOperation(
            "UPDATE `queue` SET `status` = %s WHERE `asin` = %s",
            (status, data['asin'], )
        )

    def _preprocess_item(self, item):
        item['quantity'] = int(item['quantity'])
        try:
            last_quantity = self.quantities[item['asin']][item['seller']]
        except KeyError:
            last_quantity = 0
        logger.debug('Last quantity: %s', last_quantity)
        if item.get('price', 0) and (item['quantity'] < last_quantity):
            item['sold'] = last_quantity - item['quantity']
        return item