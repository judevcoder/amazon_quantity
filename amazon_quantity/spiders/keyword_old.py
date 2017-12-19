# -*- coding: utf-8 -*-
import scrapy
import re
from scrapy.conf import settings
import random
import logging
from amazon_quantity.captcha import binarized


class KeywordSpider(scrapy.Spider):
    name = 'keyword_old'
    allowed_domains = ['amazon.com']
    start_urls = ['http://amazon.com/']
    concurrent_requests = settings.get('CONCURRENT_REQUESTS')
    keywords = []
    proxies = []
    captchas_count = 0
    pages_limit = 50
    custom_settings = {
        'ITEM_PIPELINES': {
            'amazon_quantity.pipelines.KeywordPipeline': 300
        }
    }

    def start_requests(self):
        i = 0
        while i < self.concurrent_requests:
            if not self.keywords:
                return
            keyword_data = self.keywords.pop(0)  # [keyword, asin]
            proxy = random.choice(self.proxies)
            asin = keyword_data[0]
            keyword = keyword_data[1]
            meta = {'asin': asin, 'keyword': keyword, 'cookiejar': i, 'proxy': proxy, 'page': 1}
            url = 'https://www.amazon.com' \
                  '/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords={}'.format(keyword)
            headers = {'Referer': 'https://www.amazon.com/'}
            yield scrapy.Request(url, self.get_list, meta=meta, dont_filter=True, headers=headers)
            i += 1
            logging.debug('Start: {}'.format(meta))
            # break
        return

    def get_list(self, response):
        captcha_form = response.xpath('//form[@action="/errors/validateCaptcha"]')
        if captcha_form:
            logging.debug('NEED CAPTCHA SOLVE!')
            captcha_img = captcha_form.xpath('.//img/@src').extract_first()
            rq = scrapy.Request(url=captcha_img, callback=self.get_captcha, dont_filter=True)
            rq.meta['asin'] = response.meta['asin']
            rq.meta['keyword'] = response.meta['keyword']
            rq.meta['page'] = response.meta['page']
            rq.meta['resp'] = response
            rq.meta['proxy'] = response.meta['proxy']
            rq.meta['cookiejar'] = response.meta['cookiejar']
            yield rq
            return
        if 'got_captcha' in response.meta:
            logging.info('Captcha solved')

        asin = response.meta['asin']
        meta = {'asin': asin, 'keyword': response.meta['keyword'],
                'cookiejar': response.meta['cookiejar'], 'proxy': response.meta['proxy']}
        links = response.xpath('//a[contains(@class,"s-access-detail-page")][contains(@href,"/dp/")]/@href').extract()

        search_rank = []
        for link in links:
            if asin in link:
                url = response.urljoin(link)
                search_rank = re.findall(r'&sr=\d*?-(\d*)&', url)
                # print search_rank
                break
        next_page = response.xpath('//a[@id="pagnNextLink"]/@href').extract_first()
        if not search_rank and next_page and response.meta['page'] < self.pages_limit:
            meta['page'] = response.meta['page'] + 1
            url = response.urljoin(next_page)
            url = url.split('&spIA=')[0]
            yield scrapy.Request(response.urljoin(url), self.get_list, meta=meta, dont_filter=True)
            return

        item = {
            'asin': response.meta['asin'],
            'keyword': response.meta['keyword'],
            'rank': search_rank[0] if search_rank else '0'
        }
        results_count = response.xpath('//h2[@id="s-result-count"]/text()').extract_first()
        if results_count:
            item['results_count'] = results_count.split()[2].replace(',', '')
        item['page'] = response.meta['page']
        yield item

        if not self.keywords:
            return
        keyword_data = self.keywords.pop(0)  # [keyword, asin]
        asin = keyword_data[0]
        keyword = keyword_data[1]
        meta = {'asin': asin, 'keyword': keyword, 'cookiejar': response.meta['cookiejar'],
                'proxy': response.meta['proxy'], 'page': 1}
        url = 'https://www.amazon.com' \
              '/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords={}'.format(keyword)
        headers = {'Referer': 'https://www.amazon.com/'}
        yield scrapy.Request(url, self.get_list, meta=meta, dont_filter=True, headers=headers)
        logging.debug('Start: {}'.format(meta))
        return

    def get_captcha(self, response):
        res_captcha = binarized(response.body)
        print res_captcha
        formdata = {'field-keywords': res_captcha}
        resp = response.meta['resp']
        rq = scrapy.FormRequest.from_response(resp, formdata=formdata, callback=self.get_list, dont_filter=True)
        rq.meta['asin'] = response.meta['asin']
        rq.meta['keyword'] = response.meta['keyword']
        rq.meta['page'] = response.meta['page']
        rq.meta['proxy'] = response.meta['proxy']
        rq.meta['cookiejar'] = response.meta['cookiejar']
        rq.meta['got_captcha'] = True
        yield rq
        return
