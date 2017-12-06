# Extract text from filename
class enhance_extract_hashtags(object):

	def process (self, parameters={}, data={} ):

		minimallenght = 3
		
		data['hashtag_ss'] = [ word for word in data['content'].split() if (word.startswith("#") and len(word) > minimallenght) ]
	
		return parameters, data
		
