#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os.path
import sys

from etl_file import Connector_File
from tasks import index_file

#
# Parallel processing of files by adding each file to celery tasks
#

class Connector_Filedirectory(Connector_File):
	
	def __init__(self, verbose=False, quiet=False):

		Connector_File.__init__(self, verbose=verbose)

		self.quiet = quiet


	# index directory
	def index(self, filename):
	
		# clean filename (convert filename given as URI to filesystem)
		filename = self.clean_filename(filename)
	
		# if singe file start to index it
		if os.path.isfile(filename):
			
			if not self.quiet:
				print ("Adding file to queue: {}".format(filename))

			index_file.delay(filename = filename, config=self.config)
			
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

	
	# walk trough all sub directories and call index_file for each file
	def index_dir (self, rootDir, followlinks=False):
		
		for dirName, subdirList, fileList in os.walk(rootDir, followlinks=followlinks):
	
			if self.verbose:
				print("Scanning directory: {}".format(dirName))
	
				
			for fileName in fileList:
				if self.verbose:
					print("Scanning file: {}".format(fileName))
	
				try:
					
					fullname = dirName
					if not fullname.endswith(os.path.sep):
						fullname += os.path.sep
					fullname += fileName

					if not self.quiet:
						print ("Adding file to queue: {}".format(fullname))
					index_file.delay(filename = fullname, config=self.config)
												
				except KeyboardInterrupt:
					raise KeyboardInterrupt
				except BaseException as e:
					try:
						sys.stderr.write( "Exception while processing file {}{}{} : {}\n".format(dirName, os.path.sep, fileName, e) )
					except:
						sys.stderr.write( "Exception while processing a file and exception while printing error message (maybe problem with encoding of filename on console or converting the exception to string?)\n" )


#
# Read command line arguments and start
#

#if running (not imported to use its functions), run main function
if __name__ == "__main__":

	from optparse import OptionParser 

	#get uri or filename from args

	parser = OptionParser("etl-file [options] filename")
	parser.add_option("-q", "--quiet", dest="quiet", action="store_true", default=None, help="Don\'t print status (filenames) while indexing")
	parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=None, help="Print debug messages")
	parser.add_option("-f", "--force", dest="force", action="store_true", default=None, help="Force (re)indexing, even if no changes")
	parser.add_option("-c", "--config", dest="config", default=False, help="Config file")
	parser.add_option("-p", "--plugins", dest="plugins", default=False, help="Plugins (comma separated and in order)")
	parser.add_option("-w", "--outputfile", dest="outputfile", default=False, help="Output file")

	(options, args) = parser.parse_args()
	

	if len(args) < 1:
		parser.error("No filename given")

	connector = Connector_Filedirectory()

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

