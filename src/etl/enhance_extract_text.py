import subprocess


#
# Extract text from filename
#

class enhance_extract_text(object):

	def process (self, parameters={}, data={} ):

		filename = parameters['filename']

		if 'tika' in parameters:
			tika = parameters['tika']
		else:
			tika = '/usr/share/java/tika-app.jar'
			

		#extract text with tika app
		text = subprocess.check_output(['java', '-jar', tika, '--encoding=UTF-8', '--text', filename])
		data['content'] = text

		return parameters, data
		