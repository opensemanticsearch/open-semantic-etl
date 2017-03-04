#!/usr/bin/python
# -*- coding: utf-8 -*-

import os.path
import sys
import threading

from etl import ETL



class Connector_File(ETL):
	
	def __init__(self, verbose=False, quiet=True):

		ETL.__init__(self, verbose=verbose)

		self.quiet = quiet

		self.set_configdefaults()
		
		self.read_configfiles()

		# if not set explicit, autodetection of count of CPUs for amount of threads
		if not self.threads_max:
			import multiprocessing
			self.threads_max = multiprocessing.cpu_count()
			if self.verbose:
				print ( "Setting threads to count of CPUs: " + str(self.threads_max) )

		self.e_job_done = threading.Event()

	def set_configdefaults(self):
		#
		# Standard config
		#
		# Do not edit config here! Overwrite options in /etc/etl/ or /etc/opensemanticsearch/connector-files
		#
		
		ETL.set_configdefaults(self)
		
		self.threads_max = None
				
		self.config['force'] = False

		# filename to uri mapping
		self.config['uri_prefix_strip'] = None;
		self.config['uri_prefix'] = 'file://'
		
		self.config['facet_path_strip_prefix'] = [ "file://" ]
		
		self.config['plugins'] = [
			'enhance_mapping_id',
			'filter_blacklist',
			'filter_file_not_modified',
			'enhance_extract_text_tika_server',
			'enhance_pst',
			'enhance_csv',
			'enhance_file_mtime',
			'enhance_path',
			'enhance_zip',
			'clean_title'
		]
		
		self.config['blacklist'] = ["/etc/opensemanticsearch/blacklist/blacklist-url"]
		self.config['blacklist_prefix'] = ["/etc/opensemanticsearch/blacklist/blacklist-url-prefix"]
		self.config['blacklist_suffix'] = ["/etc/opensemanticsearch/blacklist/blacklist-url-suffix"]
		self.config['blacklist_regex'] = ["/etc/opensemanticsearch/blacklist/blacklist-url-regex"]
		self.config['whitelist'] = ["/etc/opensemanticsearch/blacklist/whitelist-url"]
		self.config['whitelist_prefix'] = ["/etc/opensemanticsearch/blacklist/whitelist-url-prefix"]
		self.config['whitelist_suffix'] = ["/etc/opensemanticsearch/blacklist/whitelist-url-suffix"]
		self.config['whitelist_regex'] = ["/etc/opensemanticsearch/blacklist/whitelist-url-regex"]
	


	def read_configfiles(self):
		#
		# include configs
		#
		
		# Windows style filenames
		self.read_configfile ('conf\\opensemanticsearch-etl')
		self.read_configfile ('conf\\opensemanticsearch-enhancer-rdf')
		self.read_configfile ('conf\\opensemanticsearch-connector-files')
		
		
		# Linux style filenames
		self.read_configfile ('/etc/opensemanticsearch/etl')
		self.read_configfile ('/etc/opensemanticsearch/enhancer-rdf')
		self.read_configfile ('/etc/opensemanticsearch/connector-files')
		


	# clean filename (convert filename given as URI to filesystem)
	def clean_filename(self, filename):

		# if exist delete prefix file://

		if filename.startswith("file://"):
			filename = filename.replace("file://", '', 1)

		return filename



	# index directory or file
	def index(self, filename):
	
		# clean filename (convert filename given as URI to filesystem)
		filename = self.clean_filename(filename)
	
		# if singe file start to index it
		if os.path.isfile(filename):

			self.index_file(filename=filename)
			
			result = True
				
		# if directory walkthrough
		elif os.path.isdir(filename):
			
			self.index_dir(rootDir=filename)
			
			result = True
	
		# else error message
		else:

			result = False

			sys.stderr.write( "No such file or directory: {}\n".format(filename) )
	
		return result


	
	# walk trough all sub directories and call indexfile on each file
	def index_dir (self, rootDir, followlinks=False):
		
		for dirName, subdirList, fileList in os.walk(rootDir, followlinks=followlinks):
	
			if self.verbose:
				print('Scanning directory: %s' % dirName)
	
				
			for fileName in fileList:
				if self.verbose:
					print('Scanning file: %s' % fileName)
	
				try:
					
					fullname = dirName + os.path.sep + fileName

					# no threading					
					if self.threads_max == 1:

						self.index_file( filename = fullname )


					else:

						# open new thread or sleep if yet maximum threads (but do not count this main thread) running
						while threading.active_count() >= self.threads_max + 1:
							self.e_job_done.wait(1)

	
						thread = threading.Thread( target=self.index_file, args=( fullname, ) )
						# reset signal event we will wait for which will be set/signaled after work / thread was done
						self.e_job_done.clear()
						thread.start()
					
					
				except KeyboardInterrupt:
					raise KeyboardInterrupt
				except BaseException as e:
					try:
						sys.stderr.write( "Exception while processing file {}{}{} : {}\n".format(dirName, os.path.sep, fileName, e) )
					except:
						sys.stderr.write( "Exception while processing a file and exception while printing error message (maybe problem with encoding of filename on console or converting the exception to string?)\n" )


	
	# Index a file
	def index_file(self, filename):
	
		# clean filename (convert filename given as URI to filesystem)
		filename = self.clean_filename(filename)
	
		# fresh parameters / chain for each file (so processing one file will not change config/parameters for next, if directory or multiple files, which would happen if given by reference)
		parameters = self.config.copy()

		if self.verbose:
			parameters['verbose'] = True
	
		data = {'content_type': 'Unknown'}
	
	
		if not 'id' in parameters:
			parameters['id'] = filename

		parameters ['filename'] = filename
	
	
		parameters, data = self.process( parameters=parameters, data=data)
		

		# set "done" signal event, so main thread wakes up and knows next job/thread can be started
		self.e_job_done.set()


#
# Read command line arguments and start
#

#if running (not imported to use its functions), run main function
if __name__ == "__main__":

	from optparse import OptionParser 

	#get uri or filename from args

	parser = OptionParser("etl-file [options] filename")
	parser.add_option("-q", "--quiet", dest="quiet", action="store_true", default=None, help="Dont print status (filenames) while indexing")
	parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=None, help="Print debug messages")
	parser.add_option("-f", "--force", dest="force", action="store_true", default=None, help="Force (re)indexing, even if no changes")
	parser.add_option("-c", "--config", dest="config", default=False, help="Config file")
	parser.add_option("-p", "--plugins", dest="plugins", default=False, help="Plugins (comma separated and in order)")
	parser.add_option("-w", "--outputfile", dest="outputfile", default=False, help="Output file")

	(options, args) = parser.parse_args()
	

	if len(args) < 1:
		parser.error("No filename given")

	connector = Connector_File()

	connector.read_configfile ('/etc/etl/config')

	# add optional config parameters
	if options.config:
		connector.read_configfile(options.config)

	if options.outputfile:
		connector.config['outputfile'] = options.outputfile

	# set (or if config overwrite) plugin config
	if options.plugins:
		connector.config['plugins'] = options.plugins.split(',')

	if options.verbose == False or options.verbose==True:
		connector.verbose = options.verbose
		
	if options.quiet == False or options.quiet==True:
		connector.quiet = options.quiet

	if options.force == False or options.force==True:
		connector.config['force'] = options.force

	# index each filename
	for filename in args:
		connector.index(filename)


	# wait until all started threads done (event will signalize after a job done)
	while threading.active_count() > 1:
		connector.e_job_done.wait(1)
		connector.e_job_done.clear()

	# commit changes, if not yet done automatically by Index timer
	connector.commit()
