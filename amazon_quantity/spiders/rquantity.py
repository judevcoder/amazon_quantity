# -*- coding: utf-8 -*-
import logging
import pprint
import random
import re
import urlparse
import time

from scrapy import FormRequest, Request, Spider
from scrapy.conf import settings

from amazon_quantity.utils import get_etree, clone_from_response, load_from_dict, get_data, create_connection
from amazon_quantity.presets import site_xpath, item_xpath, offer_xpath, product_xpath
from amazon_quantity.items import SpiderLoader, OfferItem


logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class RAmazonSpider(Spider):
    name = 'rquantity'
    cookiejar = 1
    asins_done = set()
    scrape_variations = settings.get('SCRAPE_VARIATIONS')
    scrape_offers = settings.get('SCRAPE_OFFERS')
    custom_settings = {
        'ITEM_PIPELINES': {
            'amazon_quantity.rpipelines.QuantifyPipeline': 300
        },
        'SPIDER_MIDDLEWARES': {
            'amazon_quantity.middlewares.AmazonCaptchaMiddleware': 909,
        }
    }
    url_template = 'https://www.amazon.com/dp/{}?_encoding=UTF8&psc=1'
    default_referer = 'https://www.google.com/'
    max_quantity = '1000'
    max_add_product_retries = 3
    save_html = False
    timestamp = time.time()
    mysql = None
    jar_cnt = 0

    def start_requests(self):
        self.mysql = create_connection(settings)
        for item in ['scrapy.dupefilters', ]:
            logging.getLogger(item).setLevel('INFO')
        # TODO proxy can be moved to download middleware
        asins = tuple(p['asin'] for p in get_data("SELECT `asin` from `queue` WHERE `status` = 1", self.mysql))
        proxies = tuple(p['proxy'] for p in get_data("SELECT `proxy` from `proxies` WHERE `status` = 1", self.mysql))
        logger.info('ASINs found: %s', len(asins))
        for asin in asins:
            try:
                previous_quantity = [get_data(
                    """
                    SELECT quantity
                    FROM `products`
                    WHERE asin = '{}'
                    ORDER BY updated DESC
                    LIMIT 1
                    """.format(asin), self.mysql
                )[0]['quantity'], ]
            except IndexError:  # New item
                previous_quantity = [None, ]
            rq = Request(
                self.url_template.format(asin),
                headers={'Referer': self.default_referer}
            )
            self.jar_cnt += 1
            rq.meta.update({
                'item': {'source_asin': asin, 'quantity': '0', '_previous_quantity': previous_quantity},
                'proxy': random.choice(proxies),
                'cookiejar': self.jar_cnt,
            })
            yield rq
            self.asins_done.add(asin)

    def parse(self, response):
        etree = get_etree(response)
        item = response.meta['item']
        item['asin'] = re.findall(r'/dp/(\w+)', response.url)[0]
        item['url'] = response.url
        for k, xpath in item_xpath.items():
            item[k] = etree.xpath(xpath)
        if etree.xpath(site_xpath['add_to_card']):
            formdata = {field.name: field.value for field in etree.xpath(".//form[@id='addToCart']/input")}
            formdata.update({'quantity': self.max_quantity})
            yield clone_from_response(
                FormRequest.from_response(
                    response,
                    formid="addToCart",
                    formdata=formdata,
                    callback=self.add_product,
                    dont_filter=True,
                ),
                response, {'_loader': SpiderLoader()}
            )
            url = etree.xpath(site_xpath['link'])
            if url and self.scrape_offers:
                yield clone_from_response(Request(url, self.get_offers), response, {'_response': response})
        else:
            logger.info('No add button for %s', response.url)
            yield load_from_dict(SpiderLoader(), item)
        if self.scrape_variations:
            asins = {asin.split(',')[-1] for asin in etree.xpath(site_xpath['drop_down_asins'])} \
                    | {asin.split('/')[2] for asin in etree.xpath(site_xpath['twister_asins'])} \
                    | set(etree.xpath(site_xpath['link_asins'])) \
                    - self.asins_done
            for asin in asins:
                yield clone_from_response(Request(self.url_template.format(asin)), response)
                self.asins_done.add(asin)

    def get_offers(self, response):
        etree = get_etree(response)
        base_response = response.meta['item']['_response']
        for offer in etree.xpath(offer_xpath['row']):
            item = response.meta['item'].copy()
            item['seller'] = offer.xpath(offer_xpath['seller'])
            item['price'] = offer.xpath(offer_xpath['price'])
            item['stars'] = offer.xpath(offer_xpath['stars'])
            if offer.xpath(offer_xpath['form']):
                yield clone_from_response(
                    FormRequest.from_response(
                        base_response,
                        formid="addToCart",
                        formdata={'quantity': self.max_quantity, 'offerListingID': offer.xpath(offer_xpath['offering_id'])},
                        callback=self.add_product),
                    response, {'_loader': SpiderLoader(item=OfferItem())}
                )
            else:
                logger.info('No add button for %s', response.url)
                yield load_from_dict(SpiderLoader(item=OfferItem()), item)
            next_page = etree.xpath(offer_xpath['next_page'])
            if next_page:
                yield clone_from_response(
                    Request(next_page, self.get_offers),
                    response, {'_response': base_response},
                )

    def add_product(self, response):
        retries = response.meta.get('_add_product_retries', 0)
        item = response.meta['item']
        if retries:
            logger.info('Retry %s for %s', retries, item['url'])
        loader = item['_loader']
        etree = get_etree(response)
        try:
            item['quantity'] = re.findall(r' \((\d+) items?\)', etree.xpath(product_xpath['quantity']))
            quantity_value = int(item['quantity'][0])
        except Exception:
            logger.exception("Can't parse quantity for %s", item['asin'])
        else:
            if not quantity_value:
                logger.warning(
                    'Zero quantity found for %s, retry %s, payload:\n %s',
                    item['url'], retries,
                    pprint.pformat(dict(urlparse.parse_qsl(response.request.body)))
                )
                if retries != self.max_add_product_retries:
                    request = clone_from_response(
                        Request(
                            self.url_template.format(item['asin']),
                            headers={'Referer': self.default_referer},
                            dont_filter=True
                        ),
                        response,
                    )
                    request.meta['_add_product_retries'] = retries + 1
                    self.jar_cnt += 1
                    request.meta['cookiejar'] = self.jar_cnt
                    return request
            if response.meta['item']['_previous_quantity'][0]:  # Existed item
                response.meta['item']['_previous_quantity'].append(quantity_value)
                if response.meta['item']['_previous_quantity'][-1] != response.meta['item']['_previous_quantity'][-2]:
                    # quantity changed or we got different results in 2 last requests
                    logger.info(
                        'Quantity changes for %s : %s',
                        response.meta['item']['asin'],
                        response.meta['item']['_previous_quantity']
                    )
                    request = response.request.copy()
                    request.meta['item']['_previous_quantity'] = response.meta['item']['_previous_quantity']
                    self.jar_cnt += 1
                    request.meta['cookiejar'] = self.jar_cnt
                    return request  # Try again
                elif len(response.meta['item']['_previous_quantity']) > 2:
                    logger.debug(
                        'Quantity changes for %s : %s',
                        response.meta['item']['asin'],
                        response.meta['item']['_previous_quantity']
                    )
            if self.save_html:
                with open('html/{}_{}.html'.format(item['asin']+str(), self.timestamp), 'w') as f:
                    f.write(response.body)
            return load_from_dict(loader, item)