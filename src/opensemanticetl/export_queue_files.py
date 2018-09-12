#
# Write filename to Celery queue for batching and parallel processing
#

from tasks import index_file

class export_queue_files(object):

	def __init__(self, config = {'verbose': False} ):
		
		self.config = config	

	
	def process (self, parameters={}, data={} ):

		# write to queue
		index_file.delay( filename = parameters['filename'] )

		# Since will be don't process further / export to index
		parameters['break'] = True
	
		return parameters, data
