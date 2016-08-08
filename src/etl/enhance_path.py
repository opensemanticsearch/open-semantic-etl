#
# Build and add path facets from filename
#

class enhance_path(object):
	
	def process (self, parameters={}, data={} ):
		
		docid = parameters['id']
		
		if 'facet_path_strip_prefix' in parameters:
			facet_path_strip_prefix = parameters['facet_path_strip_prefix']
		else:
			facet_path_strip_prefix = ['file://', 'http://', 'https://']
			
		# if begins with unwanted path prefix strip it
		if facet_path_strip_prefix:
			for prefix in facet_path_strip_prefix:
				if docid.startswith(prefix):
					docid = docid.replace(prefix, '', 1)
					break
	
		# replace backslash (i.e. windows filenames) with unix path seperator
		docid = docid.replace("\\", '/')
	
		# replace # (i.e. uri) with unix path seperator
		docid = docid.replace("#", '/')
	
	
		# if more then one /
		docid = docid.replace("//", '/')
	
	
		# path
		path = docid.split('/')
		
		i = 0
		for subpath in path:
			# we dont want empty values because of leading / in paths or ending / in domains
			if subpath:
				data['path' + str(i) + '_s'] = subpath
				i += 1
	
		return parameters, data
