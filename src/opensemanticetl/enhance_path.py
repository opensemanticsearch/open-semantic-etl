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
		deep = 0

		for subpath in path:
			i += 1
			# we dont want empty values because of leading / in paths or ending / in domains
			if subpath:

				# not last path element, so part of path, not the filename at the end
				if i < len(path):
					data['path' + str(deep) + '_s'] = subpath
					deep += 1

				# last element, so basename/pure filename without path
				else:
				    data['path_basename'] = subpath

	
		return parameters, data
