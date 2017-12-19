# -*- coding: utf-8 -*-
import scrapy
import re
import random
import logging
from scrapy import signals
from scrapy.conf import settings
from amazon_quantity.captcha import binarized


class AmazonSpider(scrapy.Spider):
    name = 'quantity_old'
    allowed_domains = ['amazon.com']
    start_urls = ['http://amazon.com']
    cookiejar = 1
    asins_done = []
    asins_start = []
    proxies = []
    scrape_variations = settings.get('SCRAPE_VARIATIONS')
    scrape_offers = settings.get('SCRAPE_OFFERS')
    custom_settings = {
        'ITEM_PIPELINES': {
            'amazon_quantity.pipelines.QuantityPipeline': 300
        }
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(AmazonSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        print 'DONE'

    def start_requests(self):
        jar = 0
        for asin in self.asins_start[:10]:
            proxy = random.choice(self.proxies)
            headers = {'Referer': 'https://www.google.com/'}
            rq = scrapy.Request('https://www.amazon.com/dp/{}?_encoding=UTF8&psc=1'.format(asin), headers=headers)
            rq.meta['proxy'] = proxy
            rq.meta['cookiejar'] = jar
            item = {}
            item['source_asin'] = asin
            rq.meta['item'] = item

            yield rq
            jar += 1
            self.asins_done.append(asin)
        return

    def parse(self, response):
        logging.debug(response.meta)
        captcha_form = response.xpath('//form[@action="/errors/validateCaptcha"]')
        if captcha_form:
            logging.debug('NEED CAPTCHA SOLVE!')
            captcha_img = captcha_form.xpath('.//img/@src').extract_first()
            rq = scrapy.Request(url=captcha_img, callback=self.get_captcha, dont_filter=True)
            rq.meta['resp'] = response
            rq.meta['proxy'] = response.meta['proxy']
            rq.meta['cookiejar'] = response.meta['cookiejar']
            rq.meta['item'] = response.meta['item']
            yield rq
            return
        if 'got_captcha' in response.meta:
            logging.info('Captcha solved')
        # open('st.html', 'w').write(response.body)
        item = response.meta['item']
        item['name'] = response.xpath('//span[@id="productTitle"]/text()').extract_first(default='').strip()
        item['asin'] = re.findall(r'/dp/(\w+)', response.url)[0]
        # item['image'] = \
        #    response.xpath('//div[@id="imgTagWrapperId"]/img[@id="landingImage"]/@data-old-hires').extract_first()
        price = response.xpath('//span[contains(@class,"a-color-price")]/text()').extract_first()
        if price:
            item['price'] = price.strip()[1:]
        stars = response.xpath('//span[@id="acrPopover"]/span/a/i/span/text()').extract_first()
        if stars:
            item['stars'] = stars.split()[0]
        color = response.xpath('//div[@id="variation_color_name"]/div/span/text() | '
                               '//span[@id="vodd-button-label-color_name"]/text() | '
                               '//span[@class="shelf-label-variant-name"]/text()').extract_first()
        if color:
            item['color'] = color.strip()
        size = response.xpath('//select[@id="native_dropdown_selected_size_name"]'
                              '/option[@selected]/text() | '
                              '//span[@id="vodd-button-label-size_name"]/text()').extract_first()
        if size:
            item['size'] = size.strip()
        reviews = response.xpath('//span[contains(@class,"totalReviewCount")]/text()').extract_first()
        if reviews:
            item['reviews'] = reviews.replace(',', '').replace(' ', '')
        sales_rank = response.xpath(
            '//tr[contains(./th/text(), "Best Sellers Rank")]/td/span/span[1]/text()[normalize-space(.)] | '
            '//li[@id="SalesRank"]/text()[normalize-space(.)] | '
            '//li[@id="SalesRank"]/ul/li/span/text()').extract_first()
        if sales_rank:
            item['bsr'] = ''.join(re.findall(r'#([\d,]*)', sales_rank)).replace(',', '')

        seller = response.xpath('//a[@id="bylineInfo" or @id="brand"]/text()').extract_first()
        if not seller:
            seller = response.xpath('//a[@id="brand"]/@href').extract_first(default='/').split('/')[1]
        item['seller'] = seller.strip()

        add_to_cart = response.xpath('//input[@id="add-to-cart-button"]/@value').extract()
        if add_to_cart:
            rq = scrapy.FormRequest.from_response(response, formid="addToCart", formdata={'quantity': '1000'},
                                                  callback=self.add_product)
            rq.meta['item'] = item
            rq.meta['proxy'] = response.meta['proxy']
            rq.meta['cookiejar'] = response.meta['cookiejar']
            yield rq

            link = response.xpath('//div[@id="mbc"]/div/div/div/span/a/@href').extract_first()
            if link and self.scrape_offers:
                rq = response.follow(link, self.get_offers)
                rq.meta['item'] = item
                rq.meta['proxy'] = response.meta['proxy']
                rq.meta['cookiejar'] = response.meta['cookiejar']
                rq.meta['resp'] = response
                yield rq
        else:
            item['quantity'] = 0
            yield item

        if not self.scrape_variations:
            return
        drop_down_asins = response.xpath('//div[contains(@id, "variation_") and contains(@id, "_name")]'
                                         '//select/option[not(@value="-1")]/@value').extract()
        link_asins = response.xpath('//div[contains(@id, "variation_") and contains(@id, "_name")]'
                                    '//ul/@data-defaultasin').extract()
        twister_asins = response.xpath('//form[@id="twister"]//a[contains(@href,"/dp/")]/@href').extract()
        twister_asins = [asin.split('/')[2] for asin in twister_asins]

        asins = list(set([row.split(',')[-1] for row in drop_down_asins] + link_asins + twister_asins))

        for asin in asins:
            if asin not in self.asins_done:
                rq = response.follow('https://www.amazon.com/dp/{}?_encoding=UTF8&psc=1'.format(asin))
                rq.meta['proxy'] = response.meta['proxy']
                rq.meta['cookiejar'] = response.meta['cookiejar']
                rq.meta['item'] = response.meta['item'].copy()
                yield rq
                self.asins_done.append(asin)
                return
        return

    def get_offers(self, response):
        rows = response.xpath('//div[@role="main"]/div[contains(@class,"olpOffer")]')
        for row in rows:
            item = response.meta['item'].copy()
            item['seller'] = row.xpath('./div[4]/h3/span/a/text()').extract_first()
            if not item['seller']:
                item['seller'] = row.xpath('./div[4]/h3/a/img/@alt').extract_first()
            price = row.xpath('./div[1]/span[contains(@class,"olpOfferPrice")]/text()').extract_first()
            if price:
                item['price'] = price.strip().replace('$', '').replace(',', '').replace(' ', '')
            else:
                item['price'] = '0'
            stars = row.xpath('./div[4]/p/i/span/text()').extract_first()
            if stars:
                item['stars'] = stars.split()[0]
            form = row.xpath('.//form//input[@name="submit.addToCart"]/@value').extract()
            if form:
                offering_id = row.xpath('.//form/input[contains(@name,"offeringID")]/@value').extract_first()
                print offering_id
                resp = response.meta['resp']
                formdata = {'quantity': '1000', 'offerListingID': offering_id}
                rq = scrapy.FormRequest.from_response(resp, formid="addToCart", formdata=formdata,
                                                      callback=self.add_product)
                rq.meta['item'] = item
                rq.meta['proxy'] = response.meta['proxy']
                rq.meta['cookiejar'] = response.meta['cookiejar']
                yield rq
                # break

        nextpage = response.xpath('//li[@class="a-last"]/a/@href').extract_first()
        if nextpage:
            rq = response.follow(nextpage, self.get_offers)
            rq.meta['item'] = response.meta['item'].copy()
            rq.meta['proxy'] = response.meta['proxy']
            rq.meta['cookiejar'] = response.meta['cookiejar']
            rq.meta['resp'] = response.meta['resp']
            yield rq

        return

    def add_product(self, response):
        captcha_form = response.xpath('//form[@action="/errors/validateCaptcha"]')
        if captcha_form:
            logging.debug('NEED CAPTCHA SOLVE!')
            captcha_img = captcha_form.xpath('.//img/@src').extract_first()
            rq = scrapy.Request(url=captcha_img, callback=self.get_captcha, dont_filter=True)
            rq.meta['resp'] = response
            rq.meta['proxy'] = response.meta['proxy']
            rq.meta['cookiejar'] = response.meta['cookiejar']
            rq.meta['item'] = response.meta['item']
            rq.meta['basket'] = True
            yield rq
            return
        if 'got_captcha' in response.meta:
            logging.info('Captcha solved')
            item = response.meta['item']
            rq = scrapy.Request('https://www.amazon.com/dp/{}?_encoding=UTF8&psc=1'.format(item['asin']),
                                dont_filter=True)
            rq.meta['proxy'] = response.meta['proxy']
            rq.meta['cookiejar'] = response.meta['cookiejar']
            rq.meta['item'] = item
            yield rq
            return
        # cookies = response.request.headers.getlist('Cookie')
        # print cookies
        item = response.meta['item']
        qty = response.xpath('//div[@id="hlb-subcart"]/div/span/span/text()[normalize-space(.)]').extract_first()
        if not qty:
            open('111.html', 'w').write(response.body)
            item['quantity'] = u'0'
            yield item
            return
        else:
            print qty
            qty = re.findall(r' \((\d+) items?\)', qty)
            if qty:
                item['quantity'] = qty[0]
                yield item
            else:
                open('2222.html', 'w').write(response.body)
        return

    def get_captcha(self, response):
        # open('captcha.jpg', 'wb').write(response.body)

        res_captcha = binarized(response.body)
        print res_captcha

        formdata = {'field-keywords': res_captcha}
        resp = response.meta['resp']
        if 'basket' in response.meta:
            callback = self.add_product
        else:
            callback = self.parse
        rq = scrapy.FormRequest.from_response(resp, formdata=formdata, callback=callback, dont_filter=True)
        rq.meta['proxy'] = response.meta['proxy']
        rq.meta['cookiejar'] = response.meta['cookiejar']
        rq.meta['item'] = response.meta['item']
        rq.meta['got_captcha'] = True
        yield rq

        return
