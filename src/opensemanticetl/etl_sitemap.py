#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import urllib.request
from dateutil import parser as dateparser
import re

from etl_web import Connector_Web
from opensemanticetl.tasks import index_web


class Connector_Sitemap(Connector_Web):

	def __init__(self, verbose=False, quiet=True):

		Connector_Web.__init__(self, verbose=verbose, quiet=quiet)

		self.quiet = quiet
		self.read_configfiles()
		self.queue = True


	def read_configfiles(self):
		#
		# include configs
		#
		
		# windows style filenames
		self.read_configfile ('conf\\opensemanticsearch-connector')
		self.read_configfile ('conf\\opensemanticsearch-enhancer-ocr')
		self.read_configfile ('conf\\opensemanticsearch-enhancer-rdf')
		self.read_configfile ('conf\\opensemanticsearch-connector-web')
		self.read_configfile ('conf\\opensemanticsearch-connector-rss')
		
		
		# linux style filenames
		self.read_configfile ('/etc/opensemanticsearch/etl')
		self.read_configfile ('/etc/opensemanticsearch/enhancer-ocr')
		self.read_configfile ('/etc/opensemanticsearch/enhancer-rdf')
		self.read_configfile ('/etc/opensemanticsearch/connector-web')
		self.read_configfile ('/etc/opensemanticsearch/connector-rss')
		
	
	# Import sitemap
	
	# Index every URL of the sitemap
	
	def index (self, sitemap):

		req = urllib.request.Request(sitemap)
		with urllib.request.urlopen(req) as response:
		   sitemap = response.readlines()
	
		for line in sitemap:
			urls = re.findall('<loc>(http:\/\/.+)<\/loc>', line.decode('utf-8'))

			for url in urls:

				# Download and Index the new or updated uri
				try:
					
					if self.queue:
	
						# add webpage to queue as Celery task

						if self.verbose or self.quiet==False:
							print ("Adding URL to queue: {}".format(url) )

						result = index_web.delay(uri=uri)

					else:

						# batchmode, index page after page ourselves
						if self.verbose or self.quiet==False:
							print ("Indexing {}".format(url) )

						result = Connector_Web.index(self, uri=url)

				except KeyboardInterrupt:
					raise KeyboardInterrupt	
				except BaseException as e:
					sys.stderr.write( "Exception while getting {} : {}".format(url, e) )
	
#
# If runned (not imported for functions) get parameters and start
#

if __name__ == "__main__":

	#get uri or filename from args
	from optparse import OptionParser 
	parser = OptionParser("etl-sitemap [options] uri")
	parser.add_option("-q", "--quiet", dest="quiet", action="store_true", default=False, help="Don't print status (filenames) while indexing")
	parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=None, help="Print debug messages")
	parser.add_option("-b", "--batch", dest="batchmode", action="store_true", default=None, help="Batch mode (Page after page instead of adding to queue)")
	parser.add_option("-c", "--config", dest="config", default=False, help="Config file")
	parser.add_option("-p", "--plugins", dest="plugins", default=False, help="Plugins (comma separated)")

	(options, args) = parser.parse_args()

	if len(args) != 1:
		parser.error("No sitemap uri(s) given")

	connector = Connector_Sitemap()

	# add optional config parameters
	if options.config:
		connector.read_configfile(options.config)

	# set (or if config overwrite) plugin config
	if options.plugins:
		connector.config['plugins'] = options.plugins.split(',')

	if options.verbose == False or options.verbose == True:
		connector.verbose = options.verbose
		
	if options.quiet == False or options.quiet == True:
		connector.quiet = options.quiet
		
	if options.batchmode == True:
		connector.queue = False

	for uri in args:
		connector.index(uri)
