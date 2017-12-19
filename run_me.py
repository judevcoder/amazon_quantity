import scrapy.cmdline
import sys

if __name__ == '__main__':
    scrapy.cmdline.execute("scrapy crawl {}".format(sys.argv[1]).split())
