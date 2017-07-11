import io
import pycurl
import sys
import json

# Extract text from filename
class enhance_extract_text_tika_server(object):

	# copy jsonresult to fieldname
	def	tikafield2datafield(self, tika_results, data, tika_fieldname, data_fieldname):

		try:
			# if field exist in jsonresults
			if tika_fieldname in tika_results[0]:
				#copy data from Tika fieldname to data fieldname
				data[data_fieldname] = tika_results[0][tika_fieldname]
		except:
			sys.stderr.write( 'Error while loading Tika field {} to field {}'.format(tika_fieldname, data_fieldname) )


	def process (self, parameters={}, data={} ):

		verbose = False
		if 'verbose' in parameters:
			if parameters['verbose']:	
				verbose = True
	
		debug = False
		if 'debug' in parameters:
			if parameters['debug']:
				debug = True
	

		if 'tika_server' in parameters:
			tika_server = parameters['tika_server']
		else:
			tika_server = 'localhost:9998'


		uri = tika_server + '/rmeta/form/text'


		httpheader = None
		if 'ocr_lang' in parameters:
			httpheader = [ 'X-Tika-OCRLanguage: ' + parameters['ocr_lang'] ]


		filename = parameters['filename']
	
		
		if verbose:
			print ("Calling Tika from {}".format(uri) )
	
		#
		# Upload file to Apache Tika uri with Curl
		#
		
		tika_result_IO = io.BytesIO()
	
		curl = pycurl.Curl()
		curl.setopt(curl.POST, 1)
		curl.setopt(curl.URL, uri)

		if httpheader:
			curl.setopt(pycurl.HTTPHEADER, httpheader)

		curl.setopt(curl.HTTPPOST, [('fileupload', (curl.FORM_FILE, filename.encode('utf-8'), curl.FORM_FILENAME, filename.encode('utf-8') ))])
		curl.setopt(curl.WRITEFUNCTION, tika_result_IO.write)
		curl.perform()

		http_status = curl.getinfo(pycurl.HTTP_CODE)

		tika_results = json.loads( tika_result_IO.getvalue().decode('utf-8') )
	
		if verbose:
			print ("CURL Status code: {}".format(http_status))
			if debug:
				print ( tika_results )
	
		curl.close()
		tika_result_IO.close()
	
	
		#
		# check returned status
		#


		# Tika returned no error
		if http_status == 200 or http_status == 204:
			
			if verbose:
				print ('Tika extracted file (HHTP-Status-Code: {}): {}'.format(http_status, filename) )
				print ('Extracted text and metadata: {}'.format(tika_results))


			self.tikafield2datafield(tika_results, data, 'Content-Type', 'content_type')
			self.tikafield2datafield(tika_results, data, 'X-TIKA:content', 'content')
			
			self.tikafield2datafield(tika_results, data, 'Author', 'author')
			# author_s is not multivalued, so if more than one author, join
			if 'author' in data:
				if isinstance(data['author'], list):
					data['author'] = ', '.join(data['author'])

			
			self.tikafield2datafield(tika_results, data, 'Content-Length', 'file_size_i')
			self.tikafield2datafield(tika_results, data, 'Content-Encoding', 'encoding_s')
			self.tikafield2datafield(tika_results, data, 'title', 'title')
			self.tikafield2datafield(tika_results, data, 'subject', 'subject')
			self.tikafield2datafield(tika_results, data, 'description', 'description')
			self.tikafield2datafield(tika_results, data, 'comments', 'comments')
			self.tikafield2datafield(tika_results, data, 'last_modified', 'last_modified')
			self.tikafield2datafield(tika_results, data, 'Keywords', 'keywords')
			self.tikafield2datafield(tika_results, data, 'Category', 'category')
			self.tikafield2datafield(tika_results, data, 'resourceName', 'resourcename')
			self.tikafield2datafield(tika_results, data, 'url', 'url')
			self.tikafield2datafield(tika_results, data, 'links', 'links')

			# email & messages
			self.tikafield2datafield(tika_results, data, 'Message-From', 'message_from_ss')
			self.tikafield2datafield(tika_results, data, 'Message-To', 'message_to_ss')
			self.tikafield2datafield(tika_results, data, 'Message-CC', 'message_cc_ss')
			self.tikafield2datafield(tika_results, data, 'Message-BCC', 'message_bcc_ss')
								
	
		# error handling
		else:
			# Tika returned error (HTTP status code <> 200)
		
			errormessage = tika_result_IO

			# todo: raise exception, so this part is done by ETL
		
			sys.stderr.write( "Error (HTTP-Status-Code: {}) while extracting {}\n".format(http_status, filename) )
			sys.stderr.write( errormessage )

			if 'error_ss' in data:
				data['error_ss'].append(errormessage)
			else:
				data['error_ss'] = [ errormessage ]

			data['error_enhance_extract_text_tika_server_t'] = errormessage

			data['content']=''
	
		return parameters, data
		