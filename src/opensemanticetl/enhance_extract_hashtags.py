# Extract text from filename
class enhance_extract_hashtags(object):

	def process (self, parameters={}, data={} ):

		data['hashtag_ss'] = [ word for word in data['content'].split() if word.startswith("#") ]
	
		return parameters, data
		