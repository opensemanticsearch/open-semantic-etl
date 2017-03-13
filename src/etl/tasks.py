#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# Files queue for batch processing and parallel processing
#

# Queue handler
from celery import Celery

# ETL connectors
from etl_file import Connector_File
from etl_delete import Delete


app = Celery('tasks')

connector_etl_file = Connector_File()
connector_etl_delete = Delete()


#
# Index a file
#

@app.task
def index_file(filename):
	connector_etl_file.index(filename=filename)


#
# Delete document with URI from index
#

@app.task
def delete(uri):
	connector_etl_delete.delete(uri=uri)
	

#
# Read command line arguments and start
#

#if running (not imported to use its functions), run main function
if __name__ == "__main__":

	from optparse import OptionParser 

	#get uri or filename from args

	parser = OptionParser("etl-tasks [options]")
	parser.add_option("-q", "--quiet", dest="quiet", action="store_true", default=None, help="Don\'t print status (filenames) while indexing")
	parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=None, help="Print debug messages")
	parser.add_option("-f", "--force", dest="force", action="store_true", default=None, help="Force (re)indexing, even if no changes")
	parser.add_option("-c", "--config", dest="config", default=False, help="Config file")
	parser.add_option("-p", "--plugins", dest="plugins", default=False, help="Plugins (comma separated and in order)")

	(options, args) = parser.parse_args()

	connector_etl_file.read_configfile ('/etc/etl/config')
	connector_etl_delete.read_configfile ('/etc/etl/config')

	# add optional config parameters
	if options.config:
		connector_etl_file.read_configfile(options.config)
		connector_etl_delete.read_configfile(options.config)

	# set (or if config overwrite) plugin config
	if options.plugins:
		connector_etl_file.config['plugins'] = options.plugins.split(',')

	if options.verbose == False or options.verbose==True:
		connector_etl_file.verbose = options.verbose
		connector_etl_delete.verbose = options.verbose
		
	if options.quiet == False or options.quiet==True:
		connector_etl_file.quiet = options.quiet

	if options.force == False or options.force==True:
		connector_etl_file.config['force'] = options.force

	
	app.worker_main()