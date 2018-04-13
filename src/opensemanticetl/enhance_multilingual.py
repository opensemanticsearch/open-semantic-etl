#
# Multilinguality
#
# Copy content language specific dynamic fields for language specific analysis like stemming, grammar or synonyms
#
# Language has been detected before by plugin enhance_detect_language using Apache Tika / OpenNLP
#

class enhance_multilingual(object):

	verbose = False

	# languages that are defined in index schema for language specific analysis and used if autodetected as documents language
	languages = ['en','fr','de','es','pt','nl','ro','it','cz','ar','fa']

	# languages for language specific analysis even if not the autodetected document language
	languages_force = []
	
	# exclude fields

	exclude_fields = ['language_s', 'encoding_s', 'content_type', 'content_type_group']

	exclude_prefix = ['etl_']

	# suffixes of non-text fields like nubers
	exclude_suffix = ['_i', '_is', '_l', '_ls', '_b','_bs','_f', '_fs', '_d','_ds','_f','_fs','_dt','_dts']


	def process (self, parameters={}, data={} ):

		if 'verbose' in parameters:
			self.verbose = parameters['verbose']

		if 'languages' in parameters:
			self.languages = parameters['languages']

		if 'languages_force' in parameters:
			self.languages_force = parameters['languages_force']

		if 'languages_exclude_fields' in parameters:
			self.exclude_fields = parameters['languages_exclude_fields']
	
		language_specific_data = {}

		# copy each data field to language specific fild with suffix _txt_$language

		for fieldname in data:

			exclude = False

			# do not copy excluded fields
			for exclude_field in self.exclude_fields:
				if fieldname == exclude_field:
					exclude = True

			for prefix in self.exclude_prefix:
				if fieldname.startswith(prefix):
					exclude = True

			for suffix in self.exclude_suffix:
				if fieldname.endswith(suffix):
					exclude = True

			if not exclude:
				# copy data to fields for language specific analysis for recognized language of document
				if "language_s" in data:
					if data['language_s']:
						language_specific_fieldname = fieldname + "_txt_" + data['language_s']
						language_specific_data[language_specific_fieldname] = to_text(data[fieldname])
						if self.verbose:
							print ( "Multilinguality: Detected document language {}, so copyied {} to {}".format(data['language_s'], fieldname, language_specific_fieldname) )

				# fields for language specific analysis forced languages even if not recognized language
				for language_force in self.languages_force:
					language_specific_fieldname = fieldname+"_txt_" + language_force
					language_specific_data[language_specific_fieldname] = to_text(data[fieldname])
					if self.verbose:
						print ( "Multilinguality: Forced language {}, so copyied {} to {}".format(language_force, fieldname, language_specific_fieldname) )

		# append language specific fields to data
		for key in language_specific_data:
			data[key] = language_specific_data[key]


		# mark the document, that it was analyzed by this plugin yet
		data['enhance_multilingual_b'] = "true"
		
		return parameters, data


# convert multivalue field to single text field
def to_text(fielddata):

	if isinstance(fielddata, list):
		texts = []
		for value in fielddata:
			texts.append( "{}".format(value) )
		text = "\n".join(texts)
	else:
		text = "{}".format(fielddata)

	return text

