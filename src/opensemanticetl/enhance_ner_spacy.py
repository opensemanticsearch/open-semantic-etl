import etl
import spacy

#
# SpaCy Named Entitiy Recognizer (NER)
#

# Appends classified (Persons, Locations, Organizations) entities (names/words) to mapped facets/fields

class enhance_ner_spacy(object):

	def process (self, parameters={}, data={} ):
	
		verbose = False
		if 'verbose' in parameters:
			if parameters['verbose']:	
				verbose = True


		if 'spacy_ner_spacy' in parameters:
			mapping = parameters['spacy_ner_spacy']
		else:
			mapping = {
			 'ORG': 'organization_ss',
			 'NORP': 'organization_ss',
			 'PER': 'person_ss',
			 'PERSON': 'person_ss',
			 'GPE': 'location_ss',
			 'LOC': 'location_ss',
			 'FACILITY': 'location_ss',
			}
	

		# default classifier
		classifier = 'en'

		if 'spacy_ner_classifier_default' in parameters:
			classifier = parameters['spacy_ner_classifier_default']

		# set language specific classifier, if configured and document language detected
		if 'spacy_ner_classifiers' in parameters and 'language_s' in data:
			# is a language speciic cassifier there for the detected language?
			if data['language_s'] in parameters['spacy_ner_classifiers']:
				classifier = parameters['spacy_ner_classifiers'][data['language_s']]

		# if standard classifier configured to None and no classifier for detected language, exit the plugin
		if not classifier:

			return parameters, data

		if verbose:
			print ("Loading SpaCY NER language / classifier: {}".format(classifier))

		nlp = spacy.load(classifier)

		analyse_fields = ['title','content','description','ocr_t','ocr_descew_t']

		text = ''
		for field in analyse_fields:
			if field in data:
				text = "{}{}\n".format(text, data[field])

		# classify/tag with class each word of the content
		
		doc = nlp(text)

		for ent in doc.ents:

		# if class of entity is mapped to a facet/field, append the entity to this facet/field
			entity = ent.text
			entity_class = ent.label_

			if entity_class in mapping:
				
				if verbose:
					print ( "NER classified word(s)/name {} to {}. Appending to mapped facet {}".format(entity, entity_class, mapping[entity_class]) )

				etl.append(data, mapping[entity_class], entity)

			else:
				if verbose:
					print ( "Since Named Entity Recognition (NER) class {} not mapped to a field/facet, ignore entity/word(s): {}".format(entity_class, entity) )


		# mark the document, that it was analyzed by this plugin yet
		data['enhance_ner_spacy_b'] = "true"
		
		return parameters, data
