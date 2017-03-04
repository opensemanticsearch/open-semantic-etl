#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# Files queue for batch processing and parallel processing
#


import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'opensemanticsearch.settings')



from etl_file import Connector_File

from celery import Celery



app = Celery('tasks',
	broker='django://')


connector = Connector_File()


@app.task
def index_file(filename):
	print (filename)
	connector.index(filename=filename)
		

#
# Read command line arguments and start
#

#if running (not imported to use its functions), run main function
if __name__ == "__main__":

	from optparse import OptionParser 

	#get uri or filename from args

	parser = OptionParser("etl-file-queue [options] filename")
	parser.add_option("-q", "--quiet", dest="quiet", action="store_true", default=None, help="Dont print status (filenames) while indexing")
	parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=None, help="Print debug messages")
	parser.add_option("-f", "--force", dest="force", action="store_true", default=None, help="Force (re)indexing, even if no changes")
	parser.add_option("-c", "--config", dest="config", default=False, help="Config file")
	parser.add_option("-p", "--plugins", dest="plugins", default=False, help="Plugins (comma separated and in order)")

	(options, args) = parser.parse_args()

	connector.read_configfile ('/etc/etl/config')

	# add optional config parameters
	if options.config:
		connector.read_configfile(options.config)

	# set (or if config overwrite) plugin config
	if options.plugins:
		connector.config['plugins'] = options.plugins.split(',')

	if options.verbose == False or options.verbose==True:
		connector.verbose = options.verbose
		
	if options.quiet == False or options.quiet==True:
		connector.quiet = options.quiet

	if options.force == False or options.force==True:
		connector.config['force'] = options.force

	
	app.worker_main()