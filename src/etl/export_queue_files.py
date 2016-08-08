#
# Write filename to celery queue for batching and parallel processing
#

import json

from tasks import index_file

class export_queue_files(object):
	

	def process (self, parameters={}, data={} ):

		# write to queue
		index_file.delay( filename = parameters['filename'] )

		# Since will be dont process further / export to index
		parameters['break'] = True
	
		return parameters, data
