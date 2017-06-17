#!/usr/bin/python3
# -*- coding: utf-8 -*-

#
# Queue tasks for batch processing and parallel processing
#

# Queue handler
from celery import Celery

# ETL connectors
from etl import ETL
from etl_delete import Delete
from etl_file import Connector_File
from etl_web import Connector_Web
from etl_rss import Connector_RSS


verbose = True

app = Celery('etl.tasks')

etl_delete = Delete()
etl_file = Connector_File()
etl_web = Connector_Web()
etl_rss = Connector_RSS()


#
# Delete document with URI from index
#

@app.task(name='etl.delete')
def delete(uri):
	etl_delete.delete(uri=uri)


#
# Index a file
#

@app.task(name='etl.index_file')
def index_file(filename, wait=0):

	if wait:
		time.sleep(wait)

	etl_file.index(filename=filename)


#
# Index a webpage
#

@app.task(name='etl.index_web')
def index_web(uri, wait=0):

	if wait:
		time.sleep(wait)

	result = etl_web.index(uri)

	return result


#
# Index RSS Feed
#

@app.task(name='etl.index_rss')
def index_rss(uri, wait=0):

	if wait:
		time.sleep(wait)

	result = etl_rss.index(uri)

	return result



#
# Enrich with / run plugins
#

@app.task(name='etl.enrich')
def enrich(plugins, uri, wait=0):
	
	if wait:
		time.sleep(wait)
	
	etl = ETL()
	etl.read_configfile('/etc/opensemanticsearch/etl')
	etl.read_configfile('/etc/opensemanticsearch/enhancer-rdf')
	
	etl.config['plugins'] = plugins.split(',')

	filename = uri

	# if exist delete protocoll prefix file://
	if filename.startswith("file://"):
		filename = filename.replace("file://", '', 1)
	
	parameters = etl.config.copy()
			
	parameters['id'] = uri
	parameters['filename'] = filename
	
	parameters, data = etl.process (parameters=parameters, data={})

	return data
	

#
# Read command line arguments and start
#

#if running (not imported to use its functions), run main function
if __name__ == "__main__":

	from optparse import OptionParser 

	parser = OptionParser("etl-tasks [options]")
	parser.add_option("-q", "--quiet", dest="quiet", action="store_true", default=False, help="Don\'t print status (filenames) while indexing")
	parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="Print debug messages")

	(options, args) = parser.parse_args()

	if options.verbose == False or options.verbose==True:
		verbose = options.verbose
		etl_delete.verbose = options.verbose
		etl_file.verbose = options.verbose
		etl_web.verbose = options.verbose
		etl_rss.verbose = options.verbose
		
	if options.quiet == False or options.quiet==True:
		etl_file.quiet = options.quiet

	app.worker_main()