# -*- coding: utf-8 -*-
import logging

from scrapy import Request, FormRequest
from scrapy.exceptions import IgnoreRequest
from twisted.internet import defer

from amazon_quantity.utils import get_etree, clone_from_response
from amazon_quantity.presets import site_xpath
from amazon_quantity.captcha import binarized

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class CaptchaError(IgnoreRequest):

    def __init__(self, response, *args, **kwargs):
        self.response = response
        super(CaptchaError, self).__init__(*args, **kwargs)


class AmazonCaptchaMiddleware(object):
    max_retries = 50
    save_fails = False

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):
        self.stats = crawler.stats

    def process_spider_input(self, response, spider):

        def check_captcha():
            etree = get_etree(response)
            return etree.xpath(site_xpath['captcha_image'])

        captcha = check_captcha()
        if captcha:
            raise CaptchaError(captcha)
        else:
            if '_captcha_retries' in response.meta:
                self.stats.inc_value('captcha/solved')
                response.meta.pop('_captcha_retries')
            return

    def process_spider_exception(self, response, exception, spider):

        @defer.inlineCallbacks
        def process_captcha(url):
            image = yield spider.crawler.engine.download(Request(url, dont_filter=True), spider)
            answer = binarized(image.body)
            logger.debug('Captcha answer is: %s', answer)
            request = clone_from_response(
                FormRequest.from_response(
                    response,
                    formdata={'field-keywords': answer},
                    callback=response.request.callback,
                    dont_filter=True,
                ),
                response,
            )
            request.meta.update({'_captcha_retries': retries + 1, '_captcha_img': image.body, '_captcha_answer': answer})
            try:
                spider.crawler.engine.crawl(request, spider)
            except Exception as e:
                logger.error(e)

        if isinstance(exception, CaptchaError):
            spider.crawler.stats.inc_value('captcha/response_dropped')
            logger.debug('Captcha detected')
            retries = response.meta.get('_captcha_retries', 0)
            if retries and self.save_fails:
                file_name = './captcha/{}.jpg'.format(response.meta['_captcha_answer'])
                with open(file_name, 'wb') as f:
                    f.write(response.meta['_captcha_img'])
                logger.debug('Not solved CAPTCHA saved to %s', file_name)
                self.stats.inc_value('captcha/fails')
            if retries != self.max_retries:
                process_captcha(exception.response)
            else:
                self.stats.inc_value('captcha/retries_exceeded')
                logger.error('Fail to solve captcha after %s retries', self.max_retries)
            return []
