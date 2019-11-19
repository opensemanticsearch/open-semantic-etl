#!/usr/bin/python3
# -*- coding: utf-8 -*-

import tempfile
import re

from scrapy.crawler import CrawlerProcess

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

from tasks import index_web


class OpenSemanticETL_Spider(CrawlSpider):

    name = "Open Semantic ETL"

    def parse_item(self, response):

        # write downloaded body to temp file
        file = tempfile.NamedTemporaryFile(
            mode='w+b', delete=False, prefix="etl_web_crawl_")
        file.write(response.body)
        filename = file.name
        file.close()

        self.logger.info(
            'Adding ETL task for downloaded page or file from %s', response.url)

        downloaded_headers = {}
        if 'date' in response.headers:
                downloaded_headers['date'] = response.headers['date'].decode("utf-8", errors="ignore")
        if 'last-modified' in response.headers:
                downloaded_headers['last-modified'] = response.headers['last-modified'].decode("utf-8", errors="ignore")

        # add task to index the downloaded file/page by ETL web in Celery task worker
        index_web.apply_async(kwargs={'uri': response.url, 'downloaded_file': filename,
                                      'downloaded_headers': downloaded_headers}, queue='tasks', priority=5)


def index(uri, crawler_type="PATH"):

    name = "Open Semantic ETL {}".format(uri)

    start_urls = [uri]

    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    if crawler_type == "PATH":
        # crawl only the path
        filter_regex = re.escape(uri) + '*'
        rules = (
            Rule(LinkExtractor(allow=filter_regex), callback='parse_item'),
        )
        process.crawl(OpenSemanticETL_Spider,
                      start_urls=start_urls, rules=rules, name=name)

    else:
        # crawl full domain and subdomains

        allowed_domain = uri
        # remove protocol prefix
        if allowed_domain.lower().startswith('http://www.'):
            allowed_domain = allowed_domain[11:]
        elif allowed_domain.lower().startswith('https://www.'):
            allowed_domain = allowed_domain[12:]
        elif allowed_domain.lower().startswith('http://'):
            allowed_domain = allowed_domain[7:]
        elif allowed_domain.lower().startswith('https://'):
            allowed_domain = allowed_domain[8:]

        # get only domain name without path
        allowed_domain = allowed_domain.split("/")[0]

        rules = (
            Rule(LinkExtractor(), callback='parse_item'),
        )
        process.crawl(OpenSemanticETL_Spider, start_urls=start_urls,
                      allowed_domains=[allowed_domain], rules=rules, name=name)

    # the start URL itselves shall be indexed, too, so add task to index the downloaded file/page by ETL web in Celery task worker
    index_web.apply_async(kwargs={'uri': uri}, queue='tasks', priority=5)

    process.start()  # the script will block here until the crawling is finished


if __name__ == "__main__":

    # get uri or filename from args

    from optparse import OptionParser

    # get uri or filename from args

    parser = OptionParser("etl-web-crawl URL")

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.error("No URL(s) given")

    for uri in args:
        index(uri)
