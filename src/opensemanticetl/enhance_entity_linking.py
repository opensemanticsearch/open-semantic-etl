import requests
import json
import etl

from entity_linking.entity_linker import Entity_Linker

#
# Named Entity Extraction by Open Semantic Entity Search API dictionary
#

class enhance_entity_linking(object):

	def process (self, parameters={}, data={} ):
	
		verbose = False
		if 'verbose' in parameters:
			if parameters['verbose']:	
				verbose = True

		entity_linking_taggers = ['all_labels_ss_tag']
		if 'entity_linking_taggers' in parameters:
			entity_linking_taggers = parameters['entity_linking_taggers']

		entity_linking_taggers_document_language_dependent = {}
		if 'entity_linking_taggers_document_language_dependent' in parameters:
			entity_linking_taggers_document_language_dependent = parameters['entity_linking_taggers_document_language_dependent']

		if 'language_s' in data:
			# is a language specific tagger there for the detected language?
			if data['language_s'] in entity_linking_taggers_document_language_dependent:
				for entity_linking_tagger in entity_linking_taggers_document_language_dependent[data['language_s']]:
					if not entity_linking_tagger in entity_linking_taggers:
						entity_linking_taggers.append(entity_linking_tagger)
		
		openrefine_server = False
		if 'openrefine_server' in parameters:
			openrefine_server = parameters['openrefine_server']

		text = ''
		for field in data:
			
			values = data[field]

			if not isinstance(values, list):
				values = [values]
			
			for value in values:
				if value:
					text = "{}{}\n".format(text, value)

		if openrefine_server:
			# use REST-API on (remote) HTTP server
			params = {'text': text}
			r = requests.post(openrefine_server, params=params)
			results = r.json()
			
		else:
			# use local Python library
			linker = Entity_Linker()
			linker.verbose = verbose

			results = linker.entities( text = text, taggers = entity_linking_taggers )
			
		if verbose:
			print ("Named Entity Linking: {}".format(results))

		for match in results:
			for candidate in results[match]['result']:
				if candidate['match']:
					for facet in candidate['type']:
						etl.append(data, facet, candidate['name'])
						etl.append(data, facet + '_uri_ss', candidate['id'])

		# mark the document, that it was analyzed by this plugin yet
		data['enhance_entity_linking_b'] = "true"
		
		return parameters, data
