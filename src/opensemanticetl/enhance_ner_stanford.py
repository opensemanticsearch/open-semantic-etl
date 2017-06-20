import etl
from nltk.tag.stanford import StanfordNERTagger


#
# Stanford Named Entitiy Recognizer (NER)
#

# Appends classified (Persons, Locations, Organizations) entities (Names/Words) to mapped facets/fields

class enhance_ner_stanford(object):

	def process (self, parameters={}, data={} ):
	
		verbose = False
		if 'verbose' in parameters:
			if parameters['verbose']:	
				verbose = True


		if 'stanford_ner_mapping' in parameters:
			mapping = parameters['stanford_ner_mapping']
		else:
			# todo: extend mapping for models with more classes like money and date
			mapping = {
			 'PERSON': 'person_ss',
			 'LOCATION': 'location_ss',
			 'ORGANIZATION': 'organization_ss',
			}
	

		if 'stanford_ner_classifier' in parameters:
			classifier = parameters['stanford_ner_classifier']
		else:
			classifier = 'english.all.3class.distsim.crf.ser.gz'


		kwargs={}

		if 'stanford_ner_java_options' in parameters:
			kwargs['java_options'] = parameters['stanford_ner_java_options']

		if 'stanford_ner_path_to_jar' in parameters:
			kwargs['path_to_jar'] = parameters['stanford_ner_path_to_jar']

	
		if 'content' in parameters:
			content = parameters['content']
		else:
			content = data['content']
	
	
		# classify/tag with class each word of the content
		st = StanfordNERTagger(classifier, encoding='utf8', verbose=verbose, **kwargs)
		entities = st.tag(content.split())


		# if class of entity is mapped to a facet/field, append the entity to this facet/field
		for entity, entity_class in entities:

			if entity_class in mapping:
				
				if verbose:
					print ( "NER classified word/name {} to {}. Appending to mapped facet {}".format(entity, entity_class, mapping[entity_class]) )

				etl.etl.append(data, mapping[entity_class], entity)

			else:
				print ( "Ignore unknown Named Entity Recognition (NER) class {} for entity/word {}, since class not mapped to a field/facet".format(entity_class, entity) )


		# mark the document, that it was analyzed by this plugin yet
		data['enhance_ner_stanford_b'] = "true"
		
		return parameters, data

