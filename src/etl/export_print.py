import pprint

class export_print(object):
	
	#
	# Print data
	#

	def process (self, parameters={}, data={} ):

		pprint.pprint(data)
	
		return parameters, data
