#!/usr/bin/python3
# -*- coding: utf-8 -*-

import json
import feedparser
import sys
import urllib

from etl_web import Connector_Web

import export_solr


class Connector_RSS(Connector_Web):

	def __init__(self, verbose=False, quiet=True):

		Connector_Web.__init__(self, verbose=verbose, quiet=quiet)

		self.quiet = quiet
		self.read_configfiles()


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
		self.read_configfile ('/etc/opensemanticsearch/etl-webadmin')
		self.read_configfile ('/etc/opensemanticsearch/etl-custom')
		self.read_configfile ('/etc/opensemanticsearch/enhancer-ocr')
		self.read_configfile ('/etc/opensemanticsearch/enhancer-rdf')
		self.read_configfile ('/etc/opensemanticsearch/connector-web')
		self.read_configfile ('/etc/opensemanticsearch/connector-rss')
		
	
	# Import Feed
	
	#
	# Import a RSS feed: If article has changed or not indexed, call download_and_index_to_solr()
	#
	def index (self, uri):

		result = True

		exporter = export_solr.export_solr()

		feed = feedparser.parse(uri)
	
		for item in feed.entries:
		
			articleuri = item.link

			#
			# Is new article or indexed in former runs?
			#
			
			doc_mtime = exporter.get_lastmodified(docid=articleuri)

			if doc_mtime:

				if self.verbose:
					print ("Article indexed before, so skip new indexing: {}".format(articleuri))

			else:
				# Download and Index the new or updated uri

				if self.verbose:
					print ("Article not in index: {}".format(articleuri))
	
				try:
					partresult = Connector_Web.index(self, uri=articleuri)
					if partresult == False:
						result = False
				except KeyboardInterrupt:
					raise KeyboardInterrupt	
				except BaseException as e:
					sys.stderr.write( "Exception while getting {} : {}".format(articleuri, e) )
	
		return result

#
# If runned (not importet for functions) get parameters and start
#

if __name__ == "__main__":

	#todo: if no protocoll, use http://

	#get uri or filename from args
	from optparse import OptionParser 
	parser = OptionParser("etl-rss [options] uri")
	parser.add_option("-q", "--quiet", dest="quiet", action="store_true", default=None, help="Dont print status (filenames) while indexing")
	parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=None, help="Print debug messages")
	parser.add_option("-c", "--config", dest="config", default=False, help="Config file")
	parser.add_option("-p", "--plugins", dest="plugins", default=False, help="Plugins (comma separated)")
	parser.add_option("-w", "--outputfile", dest="outputfile", default=False, help="Output file")

	(options, args) = parser.parse_args()

	if len(args) != 1:
		parser.error("No uri(s) given")

	connector = Connector_RSS()

	# add optional config parameters
	if options.config:
		connector.read_configfile(options.config)
	if options.outputfile:
		connector.config['outputfile'] = options.outputfile

	# set (or if config overwrite) plugin config
	if options.plugins:
		connector.config['plugins'] = options.plugins.split(',')

	if options.verbose == False or options.verbose==True:
		connector.verbose=options.verbose
		
	if options.quiet == False or options.quiet==True:
		connector.quiet=options.quiet

		
	for uri in args:
		connector.index(uri)