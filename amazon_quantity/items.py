# -*- coding: utf-8 -*-
import logging

import re
from scrapy import Item, Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import Compose, Identity

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


def strip_text(data):
    if isinstance(data, str):
        return [data.strip(), ]
    else:
        return [s.strip() for s in data if s.strip()]


class ParseItem(object):
    """Parse first of the list with parser for empty list return None"""
    def __init__(self, parser=lambda x: x):
        self.parser = parser

    def __call__(self, data):
        if len(data):
            return self.parser(data[0])
        return None


class TakeFirst(object):
    def __init__(self, default=None, validator=lambda x: x is not None and x != ''):
        self.default = default
        self.validator = validator

    def __call__(self, data):
        for value in data:
            if self.validator(value):
                return value
        return self.default


class ProductItem(Item):
    """
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `name` text NOT NULL,
        `source_asin` varchar(16) NOT NULL,
        `asin` varchar(16) NOT NULL,
        `seller` varchar(128) NOT NULL DEFAULT '',
        `color` varchar(128) NOT NULL DEFAULT '-',
        `size` varchar(128) NOT NULL DEFAULT '',
        `price` float NOT NULL DEFAULT '0',
        `bsr` int(11) NOT NULL DEFAULT '0',
        `reviews` int(11) NOT NULL DEFAULT '0',
        `stars` float NOT NULL DEFAULT '0',
        `quantity` int(11) NOT NULL,
        `sold` int(11) NOT NULL DEFAULT '0',
        `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    """
    name = Field(
        output_processor=TakeFirst(default='Not found', validator=lambda x: bool(x))
    )
    source_asin = Field()
    asin = Field()  # +
    seller = Field(output_processor=ParseItem(
        lambda x: x.split('/')[3] if x.startswith('http') else x
    ))
    color = Field()  # +
    size = Field()  # +
    price = Field(output_processor=ParseItem(
        lambda x: 0 if 'unavailable' in x else x[1:]
    ))
    bsr = Field(output_processor=ParseItem(
        lambda x: ''.join(re.findall(r'#([\d,]*)', x)).replace(',', '')
    ))
    reviews = Field(output_processor=ParseItem(
        lambda x: x.replace(',', '').replace(' ', '')
    ))
    stars = Field(output_processor=ParseItem(
        lambda x: x.split()[0]
    ))
    quantity = Field(output_processor=TakeFirst())  # +
    url = Field(input_processor=Compose(lambda x: []))
    updated = Field()
    sold = Field()


class OfferItem(ProductItem):
    price = Field(output_processor=ParseItem(
        lambda x: x.strip().replace('$', '').replace(',', '').replace(' ', '')
    ))


class SpiderLoader(ItemLoader):
    default_input_processor = Compose(strip_text)
    default_output_processor = ParseItem()
    default_item_class = ProductItem
