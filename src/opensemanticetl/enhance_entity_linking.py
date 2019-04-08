#
# Named Entity Extraction by Open Semantic Entity Search API dictionary
#


import requests
import json
import etl
import sys
import time

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

			taxonomy_fieldname = field + '_taxonomy_'+ str(i) + field_suffix
			
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

		# add taggers for stemming
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

		# collect/copy to be analyzed text from all fields
		text = ''
		for field in data:
			
			values = data[field]

			if not isinstance(values, list):
				values = [values]
			
			for value in values:
				if value:
					text = "{}{}\n".format(text, value)

		# tag all entities (by different taggers for different analyzers/stemmers)
		for entity_linking_tagger in entity_linking_taggers:

			results = {}

			retries = 0
			retrytime = 1
			retrytime_max = 120 # wait time until next retry will be doubled until reaching maximum of 120 seconds (2 minutes) until next retry
			no_connection = True
			
			while no_connection:
				try:
					if retries > 0:
						print('Retrying to connect to Solr tagger in {} second(s).'.format(retrytime))
						time.sleep(retrytime)
						retrytime = retrytime * 2
						if retrytime > retrytime_max:
							retrytime = retrytime_max
		
					# call REST API
					if openrefine_server:
						# use REST-API on (remote) HTTP server
						params = {'text': text}
						r = requests.post(openrefine_server, params=params)
						# if bad status code, raise exception
						r.raise_for_status()

						results = r.json()
						
					else:
						# use local Python library
						linker = Entity_Linker()
						linker.verbose = verbose
			
						results = linker.entities( text = text, taggers = [entity_linking_tagger], additional_result_fields = taxonomy_fields )
	
					no_connection = False
				
				except KeyboardInterrupt:
					raise KeyboardInterrupt
				
				except requests.exceptions.ConnectionError as e:
					
					retries += 1
					
					if openrefine_server:
						sys.stderr.write( "Connection to Openrefine server failed (will retry in {} seconds). Exception: {}\n".format(retrytime, e) )
					else:
						sys.stderr.write( "Connection to Solr text tagger failed (will retry in {} seconds). Exception: {}\n".format(retrytime, e) )
				
				except requests.exceptions.HTTPError as e:
					if e.response.status_code == 503:

						retries += 1
						
						if openrefine_server:
							sys.stderr.write( "Openrefine server temporary unavailable (HTTP status code 503). Will retry in {} seconds). Exception: {}\n".format(retrytime, e) )
						else:
							sys.stderr.write( "Solr temporary unavailable (HTTP status code 503). Will retry in {} seconds). Exception: {}\n".format(retrytime, e) )

					elif e.response.status_code == 400:
						no_connection = False

						# if error because of empty entity index for that tagger because no entities imported yet, no error message / index as fail
						empty_entity_index = False
						try:
							errorstatus = e.response.json()
							if errorstatus['error']['msg'] == 'field ' + entity_linking_tagger + ' has no indexed data':
								empty_entity_index = True
						except:
							pass
						
						if not empty_entity_index:
							etl.error_message(docid=parameters['id'], data=data, plugin='enhance_entity_linking', e=e)

					else:
						no_connection = False
						etl.error_message(docid=parameters['id'], data=data, plugin='enhance_entity_linking', e=e)

				except BaseException as e:
					no_connection = False
					etl.error_message(docid=parameters['id'], data=data, plugin='enhance_entity_linking', e=e)

			if verbose:
				print ("Named Entity Linking by Tagger {}: {}".format(entity_linking_tagger, results))
	
	
			# write entities from result to document facets
			for match in results:
				for candidate in results[match]['result']:
					if candidate['match']:
						for facet in candidate['type']:

							# use different facet for fuzzy/stemmed matches
							if not entity_linking_tagger == 'all_labels_ss_tag':
								# do not use another different facet if same stemmer but forced / not document language dependent
								entity_linking_tagger_withoutforceoption = entity_linking_tagger.replace('_stemming_force_', '_stemming_')
								facet = facet + entity_linking_tagger_withoutforceoption + '_ss'
							
							etl.append(data, facet, candidate['name'])
							etl.append(data, facet + '_uri_ss', candidate['id'])
							etl.append(data, facet + '_preflabel_and_uri_ss', candidate['name'] + ' <' + candidate['id'] + '>')

							if 'matchtext' in candidate:
								for matchtext in candidate['matchtext']:
									etl.append(data, facet + '_matchtext_ss', candidate['id'] + "\t" + matchtext)
							
							for taxonomy_field in taxonomy_fields:
								if taxonomy_field in candidate:
									separated_taxonomy_fields = taxonomy2fields(field=facet, data=candidate[taxonomy_field])
									for separated_taxonomy_field in separated_taxonomy_fields:
										etl.append(data, separated_taxonomy_field, separated_taxonomy_fields[separated_taxonomy_field])
	

		# mark the document, that it was analyzed by this plugin yet
		data['etl_enhance_entity_linking_b'] = "true"
		
		return parameters, data
