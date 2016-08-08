import sys
import subprocess

#
# Extract metadata from filename
#



def property2data(line, prefix, data, field):	

	try:

		value=None

		line=line.strip()
		if line.startswith(prefix):
			value = line.replace(prefix, '', 1)
			value = value.strip()
						
		if value:
			data[field] = value
				
	except BaseException as e:
		sys.stderr.write( "Exception while checking for property {}: {}".format(property, e.message ) )



class enhance_extract_metadata(object):

	def process (self, parameters={}, data={} ):

		filename = parameters['filename']


		if 'tika' in parameters:
			tika = parameters['tika']
		else:
			tika = '/usr/share/java/tika-app.jar'


		# call tika app to return metadata
		meta = subprocess.check_output(['java', '-jar', tika, '--encoding=UTF-8', '--metadata', filename])

		data['meta_t'] = meta

		for line in meta.split("\n"):

			try:
				property2data(line, 'Content-Type:', data, 'content_type')
				property2data(line, 'Author:', data, 'author')
				property2data(line, 'Content-Length:', data, 'file_size_i')
				property2data(line, 'title:', data, 'title')
				property2data(line, 'subject:', data, 'subject')
				property2data(line, 'description:', data, 'description')
				property2data(line, 'comments:', data, 'comments')
				property2data(line, 'last_modified:', data, 'last_modified')
				property2data(line, 'Keywords:', data, 'keywords')
				property2data(line, 'Category:', data, 'category')
				property2data(line, 'resourceName:', data, 'resourcename')
				property2data(line, 'url:', data, 'url')
				property2data(line, 'links:', data, 'links')
				
				# email & messages
				property2data(line, 'Message-From: ', data, 'message_from_ss')
				property2data(line, 'Message-To: ', data, 'message_to_ss')
				property2data(line, 'Message-CC: ', data, 'message_cc_ss')
				property2data(line, 'Message-BCC: ', data, 'message_bcc_ss')

			except BaseException as e:
				sys.stderr.write( "Error while extract Tika metadata line {} - Errormessage: {}\n".format(line, e.message) )


		# if no title but subject (i.e. emails), use subject as document title
		try:
			# if no field title exists, but field subject, use it
			if not 'title' in data:
				if 'subject' in data:
					data['title'] = data['subject']

			else:
				# if title emtpy and field subject exists, use subjects value
				if not data['title']:
					if 'subject' in data:
						if data['subject']:
							data['title'] = data['subject']
							
			
		except BaseException as e:
			sys.stderr.write( "Error while sorting metadata fields: {}\n".format(e.message) )

		return parameters, data
		