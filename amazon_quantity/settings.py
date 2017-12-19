# -*- coding: utf-8 -*-

# Scrapy settings for amazon_quantity project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'amazon_quantity'
SPIDER_MODULES = ['amazon_quantity.spiders']
NEWSPIDER_MODULE = 'amazon_quantity.spiders'

# RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 403]
# CLOSESPIDER_PAGECOUNT = 25
# CLOSESPIDER_ITEMCOUNT = 500

TELNETCONSOLE_ENABLED = True
LOG_LEVEL = 'INFO'
CONCURRENT_REQUESTS = 4

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:53.0) Gecko/20100101 Firefox/53.0'

# section for settings.py
DB_HOST = 'localhost'
DB_BASE = ''
DB_USER = ''
DB_PASSWORD = ''
DB_PORT = 3306
# DB_TABLE = 'products'
UPDATE_FOR_DUPLICATE = False

# Crawl all colors/variations of each ASIN in the 'queue' table. Script will make unique values
SCRAPE_VARIATIONS = False

# Crawl all sellers in case of multiple sellers for one ASIN
SCRAPE_OFFERS = False

# Keyword ranker crawling limit
KEYWORD_PAGES_LIMIT = 20

RETRY_TIMES = 10

# Debug
#LOG_LEVEL = 'WARNING'
DUPEFILTER_DEBUG = True

URLLENGTH_LIMIT = 8124

#ITEM_PIPELINES = {
#   'amazon_quantity.pipelines.MySQLPipeline': 300,
#}

# COOKIES_DEBUG = True

# Obey robots.txt rules
# ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'amazon_quantity.middlewares.AmazonQuantitySpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'amazon_quantity.middlewares.MyCustomDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See http://scrapy.readthedocs.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    'amazon_quantity.pipelines.AmazonQuantityPipeline': 300,
#}

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
