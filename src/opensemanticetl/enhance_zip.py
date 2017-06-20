import zipfile
import sys
import hashlib
import tempfile
import os
import shutil
from etl_file import Connector_File


class enhance_zip(object):

	def process (self, parameters={}, data={} ):
		
	
		verbose = False
		if 'verbose' in parameters:
			if parameters['verbose']:	
				verbose = True
	
		filename = parameters['filename']
	

		# if the processed file was extracted from a zip (parameter container was set), write container setting in data, so the link of the id/content can be set to the zip file
		if 'container' in parameters:
			if not 'container_s' in data:
				data['container_s'] = parameters['container']
		

		# if this file is a zip file, unzip it
		if zipfile.is_zipfile(filename):
			self.unzip_and_index_files(zipfilename=filename, parameters=parameters, verbose=verbose)
	
		return parameters, data



	
	# unzip all content and index each file with literal filename of the zip file in field container
	def unzip_and_index_files(self, zipfilename, parameters={}, verbose=False):
			

		# create temp dir where to unzip the archive
		
		if 'tmp' in parameters:
			system_temp_dirname = parameters['tmp']
			if not os.path.exists(system_temp_dirname):
				os.mkdir(system_temp_dirname)
		else:
			system_temp_dirname = tempfile.gettempdir()

		# we build temp dirname ourselfes instead of using system_temp_dirname so we can use configurable / external tempdirs
		h = hashlib.md5(parameters['id'])
		temp_dirname = system_temp_dirname + os.path.sep + "opensemanticetl_enhancer_zip_" + h.hexdigest()
	
		if os.path.exists(temp_dirname) == False:
			os.mkdir(temp_dirname)
	
		
		# unzip the files
		my_zip = zipfile.ZipFile(zipfilename)
		my_zip.extractall(temp_dirname)
		my_zip.close()
		

		# prepare document processing
		connector = Connector_File()
		connector.verbose = verbose
		connector.config = parameters.copy()
		
		
		# only set container if not yet set by a zip before (if this zip is inside another zip)
		if not 'container' in connector.config:
			connector.config['container'] = zipfilename

		# walk trough all unzipped directories / files and index all files				
		for dirName, subdirList, fileList in os.walk(temp_dirname):
	
			if verbose:
				print('Scanning directory: %s' % dirName)
	
				
			for fileName in fileList:
				if verbose:
					print('Scanning file: %s' % fileName)
	
				try:
					# replace temp dirname from indexed id
					zipped_dirname = dirName.replace(temp_dirname, '', 1)
					
					# build a virtual filename pointing to original zip file
					
					if zipped_dirname:
						zipped_dirname = zipped_dirname + os.path.sep
					else:
						zipped_dirname = os.path.sep
					
					connector.config['id'] = parameters['id'] + zipped_dirname + fileName
	
					unziped_filename = dirName + os.path.sep + fileName
	
					try:
	
						connector.index_file(filename = unziped_filename)
	
					except KeyboardInterrupt:
						raise KeyboardInterrupt

					except BaseException as e:
						sys.stderr.write( "Exception while indexing zipped content {} from {} : {}\n".format(fileName, connector.config['container'], e.message) )
	
					os.remove(unziped_filename)
	
				except BaseException as e:
					sys.stderr.write( "Exception while indexing file {} : {}\n".format(fileName, e.message) )
	
		shutil.rmtree(temp_dirname)
