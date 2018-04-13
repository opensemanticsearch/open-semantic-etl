import tika
from tika import parser

# Extract text from file(name)
class enhance_extract_text_tika_server(object):

	mapping = {
		'Content-Type': 'content_type',
		'Author': 'author_ss',
		'Content-Length': 'file_size_i',
		'Content-Encoding': 'encoding_s',
		'title': 'title',
		'subject': 'subject',
		'description': 'description',
		'comments': 'comments',
		'last_modified': 'last_modified',
		'Keywords': 'keywords',
		'Category': 'category',
		'resourceName': 'resourcename',
		'url': 'url',
		'links': 'links',
		'Message-From': 'message_from_ss',
		'Message-To': 'message_to_ss',
		'Message-CC': 'message_cc_ss',
		'Message-BCC': 'message_bcc_ss',
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
		
		#
		# Parse on Apache Tika Server by python-tika
		#
		if verbose:
			print ("Parsing by Tika Server on {}".format(tika_server) )
	
		parsed = parser.from_file(filename=filename, serverEndpoint=tika_server, headers=headers)

		if parsed['content']:
			data['content'] = parsed['content']

		# copy Tika fields to (mapped) data fields
		for tika_field in parsed["metadata"]:
			if tika_field in self.mapping:
				data[self.mapping[tika_field]] = parsed['metadata'][tika_field]
			else:
				data[tika_field + '_ss'] = parsed['metadata'][tika_field]
									
		return parameters, data
		