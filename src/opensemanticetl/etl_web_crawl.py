#!/usr/bin/python3
# -*- coding: utf-8 -*-

import tempfile
import re

import scrapy
from scrapy.crawler import CrawlerProcess

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

from tasks import index_web


class OpenSemanticETL_Spider(CrawlSpider):

	name = "Open Semantic ETL"

	def parse_item(self, response):

		# write downloaded body to temp file
		file = tempfile.NamedTemporaryFile(mode='w+b', delete=False, prefix="etl_web_crawl_")
		file.write(response.body)
		filename = file.name
		file.close()

		self.logger.info('Adding ETL task for downloaded page or file from %s', response.url)

		# add task to index the downloaded file/page by ETL web in Celery task worker
		index_web.delay(uri = response.url, downloaded_file=filename, downloaded_headers=response.headers)


def index(uri):

	name = "Open Semantic ETL {}".format(uri)

	start_urls = [uri]

	filter_regex = re.escape(uri) + '*'

	rules = (
		Rule( LinkExtractor(allow=filter_regex), callback='parse_item' ),
	)

	process = CrawlerProcess({
		'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
	})

	process.crawl(OpenSemanticETL_Spider, start_urls=start_urls, rules=rules, name=name)
	process.start() # the script will block here until the crawling is finished


if __name__ == "__main__":

	#get uri or filename from args

	from optparse import OptionParser 

	#get uri or filename from args

	parser = OptionParser("etl-web-crawl URL")

	(options, args) = parser.parse_args()

	if len(args) != 1:
		parser.error("No URL(s) given")

	for uri in args:
		index(uri)

