# -*- coding: utf-8 -*-
import logging
import random
import re
import urlparse

from scrapy import Request, Spider

from amazon_quantity.utils import get_etree, clone_from_response, get_data

xpath = {
    'links': '//a[contains(@class,"s-access-detail-page")][contains(@href,"/dp/")]/@href',
    'pagination': 'string(//a[@id="pagnNextLink"]/@href)',
    'results_count': 'string(.//*[@id="s-result-count"]/text())',
    'no_results': ".//*[@id='noResultsTitle']"
}

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class RKeywordSpider(Spider):
    name = 'rkeyword'
    custom_settings = {
        'ITEM_PIPELINES': {
            'amazon_quantity.rpipelines.KeywordPipeline': 300
        },
        'SPIDER_MIDDLEWARES': {
            'amazon_quantity.middlewares.AmazonCaptchaMiddleware': 909,
        },
    }
    pages_limit = None
    url_template = 'https://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords={}'
    default_referer = 'https://www.amazon.com/'
    cookiejar = 0

    def start_requests(self):
        self.pages_limit = self.settings.get('KEYWORD_PAGES_LIMIT', 20)
        proxies = tuple(p['proxy'] for p in get_data("SELECT `proxy` from `proxies` WHERE `status` = 1", settings=self.settings))
        for keyword in get_data(
                "SELECT `asin`, `keyword` from `keyword_queue` WHERE `status` = 1",
                settings=self.settings
        ):
            self.cookiejar += 1
            rq = Request(
                    self.url_template.format(keyword['keyword']),
                    headers={'Referer': self.default_referer},
                    dont_filter=True,
                )
            rq.meta.update({
                'item': {'asin': keyword['asin'], 'keyword': keyword['keyword']},
                'proxy': random.choice(proxies),
                'cookiejar': self.cookiejar,
            })
            yield rq

    def parse(self, response):
        etree = get_etree(response)
        if etree.xpath(".//*[@id='noResultsTitle']"):
            logger.info('No results for %s', response.url)
            return
        item = response.meta['item']
        if 'results_count' not in item:
            text = etree.xpath(xpath['results_count'])
            try:
                if 'over' in text:
                    raise ValueError
                numbers = re.findall('\d+', text.replace(',', ''))
                item['results_count'] = numbers[-1]
            except (IndexError, ValueError):
                logger.info('No real results "%s" for "%s" "%s"', text, item['asin'], item['keyword'])
                rq = response.request.copy()
                self.cookiejar += 1
                rq.meta['cookiejar'] = self.cookiejar
                return rq
        search_rank = '0'
        for url in etree.xpath(xpath['links']):
            if item['asin'] in url:
                try:
                    search_rank = re.findall(r'&sr=\d*?-(\d*)&', url)[0]
                except IndexError:
                    pass
                break
        next_page = etree.xpath(xpath['pagination'])
        logger.debug('Next page: "%s"', next_page)
        try:
            more = bool(
                search_rank == '0' and
                next_page and
                not int(dict(urlparse.parse_qsl(next_page))['page']) == self.pages_limit
            )
        except KeyError:
            more = False
        if more:
            return clone_from_response(
                Request(next_page.split('&spIA=')[0], dont_filter=True),
                response,
            )
        item['rank'] = search_rank
        try:
            item['page'] = int(dict(urlparse.parse_qsl(response.url))['page'])
        except KeyError:
            item['page'] = 1
        return item  # I see no reason for Items here