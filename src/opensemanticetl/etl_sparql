#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import os
import tempfile

from etl import ETL
from enhance_rdf import enhance_rdf

from SPARQLWrapper import SPARQLWrapper, XML


#
# download (part of) graph by SPARQL query from SPARQL endpoint to RDF file
#

def download_rdf_from_sparql_endpoint(endpoint, query):

    # read graph by construct query results from SPARQL endpoint
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(XML)
    results = sparql.query().convert()

    # crate temporary filename
    file = tempfile.NamedTemporaryFile()
    filename = file.name
    file.close()

    # export graph to RDF file
    results.serialize(destination=filename, format="xml")

    return filename


class Connector_SPARQL(ETL):

	def __init__(self, verbose=False, quiet=True):

		ETL.__init__(self, verbose=verbose)

		self.read_configfiles()


	def read_configfiles(self):
		#
		# include configs
		#
		
		# windows style filenames
		self.read_configfile ('conf\\opensemanticsearch-connector')
		self.read_configfile ('conf\\opensemanticsearch-enhancer-rdf')
		self.read_configfile ('conf\\opensemanticsearch-connector-sparql')
		
		
		# linux style filenames
		self.read_configfile ('/etc/opensemanticsearch/etl')
		self.read_configfile ('/etc/opensemanticsearch/enhancer-rdf')
		self.read_configfile ('/etc/opensemanticsearch/connector-sparql')
		

	# Import SPARQL
	
	def index (self, endpoint, query):

		# download (part of) graph from endpoint to temporary rdf file
		rdffilename = download_rdf_from_sparql_endpoint(endpoint=endpoint, query=query)

		parameters=self.config.copy()

		# import the triples of rdf graph by RDF plugin
		enhancer = enhance_rdf()
		enhancer.etl_graph_file(docid=endpoint, filename=rdffilename, parameters=parameters)

		os.remove(rdffilename)

#
# If runned (not imported for functions) get parameters and start
#

if __name__ == "__main__":

	#todo: if no protocoll, use http://

	#get uri or filename from args
	from optparse import OptionParser 
	parser = OptionParser("etl-sparql [options] uri query")
	parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=None, help="Print debug messages")
	parser.add_option("-c", "--config", dest="config", default=False, help="Config file")
	parser.add_option("-p", "--plugins", dest="plugins", default=False, help="Plugins (comma separated)")

	(options, args) = parser.parse_args()

	if len(args) != 2:
		parser.error("Missing parameters endpoint URI and SPARQL query")

	connector = Connector_SPARQL()

	# add optional config parameters
	if options.config:
		connector.read_configfile(options.config)

	# set (or if config overwrite) plugin config
	if options.plugins:
		connector.config['plugins'] = options.plugins.split(',')

	if options.verbose == False or options.verbose==True:
		connector.verbose=options.verbose
		
	connector.index(endpoint=args[0], query=args[1])
