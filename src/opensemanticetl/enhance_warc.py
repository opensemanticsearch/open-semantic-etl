from warcio.archiveiterator import ArchiveIterator
import hashlib
import tempfile
import os
import sys
import shutil
import time
from etl_file import Connector_File


class enhance_warc(object):

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

		if 'content_type_ss' in data:
			mimetypes = data['content_type_ss']
		else:
			mimetypes = parameters['content_type_ss']

		#if connector returns a list, use only first value (which is the only entry of the list)
		if not isinstance(mimetypes, list):
			mimetypes = [mimetypes]
		

		# if this file is a warc file, extract it
		if ( ("application/warc" in mimetypes) or (filename.lower().endswith('.warc')) or (filename.lower().endswith('.warc.gz')) ):
			self.unwarc_and_index_files(warcfilename=filename, parameters=parameters, verbose=verbose)
	
		return parameters, data

	
	# extract content of responses to tempfile and index content file
	def unwarc_and_index_files(self, warcfilename, parameters={}, verbose=False):
			
		# create temp dir where to unwarc the archive
		if 'tmp' in parameters:
			system_temp_dirname = parameters['tmp']
			if not os.path.exists(system_temp_dirname):
				os.mkdir(system_temp_dirname)
		else:
			system_temp_dirname = tempfile.gettempdir()

		# we build temp dirname ourselfes instead of using system_temp_dirname so we can use configurable / external tempdirs
		h = hashlib.md5(parameters['id'].encode('UTF-8'))
		temp_dirname = system_temp_dirname + os.path.sep + "opensemanticetl_enhancer_warc_" + h.hexdigest()
	
		if os.path.exists(temp_dirname) == False:
			os.mkdir(temp_dirname)
	
		# prepare document processing
		connector = Connector_File()
		connector.verbose = verbose
		connector.config = parameters.copy()
		
		
		# only set container if not yet set by a zip before (if this zip is inside another zip)
		if not 'container' in connector.config:
			connector.config['container'] = warcfilename

		i = 0
					
		with open(warcfilename, 'rb') as stream:
			for record in ArchiveIterator(stream):
				i += 1

				if record.rec_type == 'response':
					
					print(record.rec_headers)

					# write WARC record content to tempfile
					tempfilename = temp_dirname + os.path.sep + 'warcrecord' + str(i)
					tmpfile = open(tempfilename, 'wb')
					tmpfile.write(record.content_stream().read())
					tmpfile.close()
					
					# set last modification time of the file to WARC-Date
					try:
						last_modified = time.mktime(time.strptime(record.rec_headers.get_header('WARC-Date'), '%Y-%m-%dT%H:%M:%SZ'))
						os.utime( tempfilename, (last_modified, last_modified) )
					except BaseException as e:
						sys.stderr.write( "Exception while reading filedate to warc content {} from {} : {}\n".format(tempfilename, connector.config['container'], e) )

					# set id (URL and WARC Record ID)
					connector.config['id'] = record.rec_headers.get_header('WARC-Target-URI')+ '/' + record.rec_headers.get_header('WARC-Record-ID')

					# index the extracted file
					try:
	
						connector.index_file(filename = tempfilename)
	
					except KeyboardInterrupt:
						raise KeyboardInterrupt

					except BaseException as e:
						sys.stderr.write( "Exception while indexing warc content {} from {} : {}\n".format(tempfilename, connector.config['container'], e) )
	
					os.remove(tempfilename)
		
		shutil.rmtree(temp_dirname)
