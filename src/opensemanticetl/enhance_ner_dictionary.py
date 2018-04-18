import etl
from dictionary.matcher import Dictionary_Matcher

#
# Named Entity Extraction by Open Semantic Entity Search API dictionary
#

class enhance_ner_dictionary(object):

	def process (self, parameters={}, data={} ):
	
		matcher = Dictionary_Matcher()
	
		verbose = False
		if 'verbose' in parameters:
			if parameters['verbose']:	
				verbose = True

		analyse_fields = ['title','content','description','ocr_t','ocr_descew_t']

		text = ''
		for field in analyse_fields:
			if field in data:
				text = "{}{}\n".format(text, data[field])

		dictionaries=[]
		for facet in parameters['facets']:
			dictionary = None
			if 'dictionary' in parameters['facets'][facet]:
				dictionary = parameters['facets'][facet]['dictionary']
				if dictionary:
					dictionaries.append(dictionary)

		if dictionaries:
			if verbose:
				print ("Matching with NER dictionaries {}".format(dictionaries))

			matches = matcher.matches(text=text, dict_ids=dictionaries)
			if verbose:
				print ("NER dictionary matches: {}".format(matches))

			for facet in parameters['facets']:
				dictionary = None
				if 'dictionary' in parameters['facets'][facet]:
					dictionary = parameters['facets'][facet]['dictionary']
					if dictionary:
						data[facet] = matches[dictionary]

		# mark the document, that it was analyzed by this plugin yet
		data['enhance_ner_dictionary_b'] = "true"
		
		return parameters, data
