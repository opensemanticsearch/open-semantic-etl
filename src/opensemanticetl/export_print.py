import pprint

class export_print(object):
	
	def __init__(self, config = {'verbose': False} ):
		
		self.config = config	

	#
	# Print data
	#

	def process (self, parameters={}, data={} ):

		pprint.pprint(data)
	
		return parameters, data
