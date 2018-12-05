#
# Named Entity Extraction by Open Semantic Entity Search API dictionary
#


import requests
import json
import etl

from entity_linking.entity_linker import Entity_Linker


#
# split a taxonomy entry to separated index fields
#
def taxonomy2fields(field, data, separator="\t", field_suffix="_ss"):
	
	result = {}
	
	# if not multivalued field, convert to used list/array strucutre
	if not isinstance(data, list):
		data = [data]
	
	for taxonomy_entry in data:
	
		i = 0
		path = ''
		for taxonomy_entry_part in taxonomy_entry.split(separator):

			taxonomy_fieldname = field + str(i) + field_suffix
			
			if not taxonomy_fieldname in result:
				result[taxonomy_fieldname] = []
			
			if len(path)>0:
				path += separator

			path += taxonomy_entry_part
			
			result[taxonomy_fieldname].append(path)
			
			i += 1
	
	return result


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

		taxonomy_fields = ['skos_broader_taxonomy_prefLabel_ss']

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

			results = linker.entities( text = text, taggers = entity_linking_taggers, additional_result_fields = taxonomy_fields )
			
		if verbose:
			print ("Named Entity Linking: {}".format(results))

		for match in results:
			for candidate in results[match]['result']:
				if candidate['match']:
					for facet in candidate['type']:
						etl.append(data, facet, candidate['name'])
						etl.append(data, facet + '_uri_ss', candidate['id'])
						if 'matchtext' in candidate:
							for matchtext in candidate['matchtext']:
								etl.append(data, facet + 'matchtext_ss', candidate['id'] + "\t" + matchtext)
						
						for taxonomy_field in taxonomy_fields:
							if taxonomy_field in candidate:
								separated_taxonomy_fields = taxonomy2fields(field=facet, data=candidate[taxonomy_field])
								for separated_taxonomy_field in separated_taxonomy_fields:
									etl.append(data, separated_taxonomy_field, separated_taxonomy_fields[separated_taxonomy_field])


		# mark the document, that it was analyzed by this plugin yet
		data['enhance_entity_linking_b'] = "true"
		
		return parameters, data
