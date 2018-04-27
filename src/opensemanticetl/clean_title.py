import sys

# Replace empty title with useful info from other fields for better usability

class clean_title(object):
	

	def process (self, parameters={}, data={} ):

		#
		# if no title but subject (i.e. emails), use subject as document / result title
		#
		
		try:
			# if no field title exists, but field subject, use it
			if not 'title_txt' in data:
				if 'subject_ss' in data:
					data['title_txt'] = data['subject_ss']
	
			else:
				# if title empty and field subject exists, use subjects value
				if not data['title_txt']:
					if 'subject_ss' in data:
						if data['subject_ss']:
							data['title_txt'] = data['subject_ss']

		except:
			sys.stderr.write( "Error while trying to clean empty title with subject" )


		# if no title yet, use the filename part of URI
		try:
			# if no field title exists, but field subject, use it
			if not 'title_txt' in data:

				# get filename from URI
				filename = parameters['id'].split('/')[-1]
				
				data['title_txt'] = filename

		except:
			sys.stderr.write( "Error while trying to clean empty title with filename" )

		
		return parameters, data
		