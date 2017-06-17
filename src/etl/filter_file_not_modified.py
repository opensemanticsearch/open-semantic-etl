#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import datetime
import sys
import importlib
#import enhance_rdf

class filter_file_not_modified(object):

	def __init__(self):

		self.verbose = False
		self.quiet = False

	def process (self, parameters={}, data={} ):

		if 'verbose' in parameters:
			if parameters['verbose']:	
				self.verbose = True

		if 'quiet' in parameters:
			self.quiet=parameters['quiet']
			
		filename=parameters['filename']
		
		# check if file size and file date are the same in DB
		# if exist delete protocoll prefix file://
		if filename.startswith("file://"):
			filename = filename.replace("file://", '', 1)
	
	
		# if relative path change to absolute path
		filename = os.path.abspath(filename)
	
	
		# get id
		docid = parameters['id']
		
		
		#get modification time from file
		file_mtime = os.path.getmtime(filename)
				
		
		export = False
		indexed_doc_mtime = None
		
		# use abstracted function from exporter module to get last modification time of file in index
		if 'export' in parameters:
			export = parameters['export']		
			module = importlib.import_module(export)
			objectreference = getattr(module, export)
			exporter = objectreference()

			#get modtime from document saved in Index
		
			indexed_doc_mtime = exporter.get_lastmodified(docid=docid)
			

		file_mtime_masked = datetime.datetime.fromtimestamp( file_mtime ).strftime( "%Y-%m-%dT%H:%M:%SZ" )

	
		# Is it a new file (not indexed so the initial 0 different to filemtime)
		# or modified (also doc_mtime <> file_mtime of file)?
	
		if indexed_doc_mtime == file_mtime_masked:
		# Doc was found in index and field moddate of solr doc same as files mtime
		# so file was indexed before and is unchanged
			doindex = False
			
			# print status
			if self.verbose:
				try:
					print("Not indexing unchanged file: {}".format(filename) )
				except:
					sys.stderr.write( "Not indexing unchanged file but exception while printing message (problem with encoding of filename or console?)" )
			
		else:
			doindex = True

			# print status, if new document
			if self.verbose or self.quiet == False:
	
				if indexed_doc_mtime == None:
					try:
						print("Indexing new file: {}".format(filename) )
					except:
						sys.stderr.write( "Indexing new file but exception while printing message (problem with encoding of filename or console?)" )
				else:
					try:
						print('Indexing modified file: {}'.format(filename) )
					except:
						sys.stderr.write( "Indexing modified file. Exception while printing filename (problem with encoding of filename or console?)\n" )

	
		# but check, if newer metadata, so we should index with them again
	
#		if not doindex:
		
#			if parameters['metaserver']:
#				meta_modified = enhance_rdf.getmeta_modified(parameters['metaserver'], docid)
#				if meta_modified and meta_modified != solr_meta_mtime:
#					doindex = True

#					if self.verbose:
#						print('Do indexing because metadata on server changed')

		
		# If force option, do further processing even if not changed
		if doindex == False and parameters['force']:
			doindex = True

			# print status
			if self.verbose or self.quiet == False:
				try:
					print('Forced indexing of unchanged file: {}'.format(filename) )
				except:
					sys.stderr.write( "Forced indexing of unchanged file but exception while printing message (problem with encoding of filename or console? Is console set to old ASCII standard instead of UTF-8?)" )


		# if not modifed, stop process, because all done on last run
		if not doindex:
			parameters['break'] = True

			
	
		return parameters, data
