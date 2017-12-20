import sys
import requests

# Extract text from filename
class enhance_detect_language_tika_server(object):

	def process (self, parameters={}, data={} ):

		verbose = False
		if 'verbose' in parameters:
			if parameters['verbose']:	
				verbose = True
	
		if 'tika_server' in parameters:
			tika_server = parameters['tika_server']
		else:
			tika_server = 'http://localhost:9998'

		uri = tika_server + '/language/string'

		analyse_fields = ['title','content','description','ocr_t','ocr_descew_t']

		text = ''
		for field in analyse_fields:
			if field in data:
				text = "{}{}\n".format(text, data[field])

		if verbose:
			print ("Calling Tika from {}".format(uri) )
		r = requests.put(uri, data=text.encode('utf-8'))

		language = r.content.decode('utf-8')

		if verbose:
			print ( "Detected language: {}".format(language) )

		data['language_s'] = language
	
		return parameters, data
