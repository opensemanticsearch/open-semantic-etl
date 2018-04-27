import tika
from tika import parser

# Extract text from file(name)
class enhance_extract_text_tika_server(object):

	mapping = {
		'Content-Type': 'content_type_ss',
		'Author': 'author_ss',
		'Content-Length': 'Content-Length_i',
		'Content-Encoding': 'Content-Encoding_s',
		'title': 'title_txt',
		'last_modified': 'last_modified_dt',
	}

	def process (self, parameters={}, data={} ):

		verbose = False
		if 'verbose' in parameters:
			if parameters['verbose']:	
				verbose = True
	
		debug = False
		if 'debug' in parameters:
			if parameters['debug']:
				debug = True

		filename = parameters['filename']
		
		if 'tika_server' in parameters:
			tika_server = parameters['tika_server']
		else:
			tika_server = 'http://localhost:9998'
		
		headers = None
		if 'ocr_lang' in parameters:
			headers = { 'X-Tika-OCRLanguage': parameters['ocr_lang'] }
		
		if 'ocr' in parameters:
			if parameters['ocr'] == False:
				headers = { 'X-Tika-OCRTesseractPath': '/False' }
		
		#
		# Parse on Apache Tika Server by python-tika
		#
		if verbose:
			print ("Parsing by Tika Server on {}".format(tika_server) )
	
		parsed = parser.from_file(filename=filename, serverEndpoint=tika_server, headers=headers)

		if parsed['content']:
			data['content_txt'] = parsed['content']

		# copy Tika fields to (mapped) data fields
		for tika_field in parsed["metadata"]:
			if tika_field in self.mapping:
				data[self.mapping[tika_field]] = parsed['metadata'][tika_field]
			else:
				data[tika_field + '_ss'] = parsed['metadata'][tika_field]
									
		return parameters, data
		