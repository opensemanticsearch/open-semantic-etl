#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import time
import feedparser
import sys
import urllib
from dateutil import parser as dateparser

from etl_web import Connector_Web

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
		self.read_configfile ('/etc/opensemanticsearch/connector')
		self.read_configfile ('/etc/opensemanticsearch/enhancer-ocr')
		self.read_configfile ('/etc/opensemanticsearch/enhancer-rdf')
		self.read_configfile ('/etc/opensemanticsearch/connector-web')
		self.read_configfile ('/etc/opensemanticsearch/connector-rss')
		
	
	# Import Feed
	
	#
	# Import a rss feed: If article has changed or not indexed, call download_and_index_to_solr()
	#
	def index (self, uri):
	
		result = True
		# todo: result to false if getting/parsing uri failed
		feed = feedparser.parse(uri)
	
		for item in feed.entries:
		
			articleuri = item.link
		
			# get mapped doc id
			docid = self.map_id(articleuri)
			
			#get modification time from file todo: from download
			try:
	
				mtime = dateparser.parse(item.published)
	
				# maybe there was a update
				try:
					if item.updated:
						mtime = dateparser.parse(item.updated)
				except BaseException as e:
					sys.stderr.write( "Exception while parsing updated date. Status: {}\n".format(e.message) )
	
			except BaseException as e:
				sys.stderr.write( "Exception while parsing date. Status: {}\n".format(e.message) )
	
			#convert mtime to solr format
	
			if mtime:
				mtime_masked = mtime.strftime("%Y-%m-%dT%H:%M:%SZ")
			else:
				mtime = time.localtime()
				mtime_masked = time.strftime("%Y-%m-%dT%H:%M:%SZ", mtime)
	
	
			#get modtime from solr document
			doc_mtime = 0
			solruri = self.config['solr'] + 'get?id=' + urllib.quote( docid ) + '&fl=file_modified_dt'
		
			doc = json.load(urllib.urlopen(solruri))
		
			if doc['doc']:
				doc_mtime = doc['doc']['file_modified_dt']
		
			#
			# Is new article (not indexed so initial 0) or modified (doc_mtime <> mtime of file)?
			#
			
	
			if mtime_masked > doc_mtime:
				# Index the article, because new or changed
				doindex = True
		
				if doc_mtime==0:
					if self.verbose or self.quiet==False:
						print ("Indexing new article {}".format(docid) )
				else:
					if self.verbose or self.quiet==False:
						print ("Indexing modified article {}".format(docid) )
		
			else:
			
				# Doc found in solr and field moddate of solr doc same as files mtime
				# so file was indexed as newest version before
				doindex = False;
		
				if self.verbose:
					print "Not indexing unchanged article {}".format ( unicode(docid) )
	
	
			# Download and Index the new or updated uri
			if doindex:
				
				try:
					partresult = Connector_Web.index(self, uri=articleuri, last_modified=False)
					if partresult == False:
						result = False
				except KeyboardInterrupt:
					raise KeyboardInterrupt	
				except BaseException as e:
					sys.stderr.write( "Exception while getting {} : {}".format(articleuri, e.message) )
	
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