#
# Write filename to Celery queue for batching and parallel processing
#

from tasks import index_file

class export_queue_files(object):

	def __init__(self, config = {'verbose': False} ):
		
		self.config = config

	
	def process (self, parameters={}, data={} ):

		# add file to ETL queue with standard priorizazion
		# but don't if only plugins not runned that should be runned later with lower priority (which will be added to queue in step below)
		if not 'only_additional_plugins_later' in parameters:
			index_file.apply_async( kwargs={ 'filename': parameters['filename'] }, queue='tasks', priority=5 )

		# add file to (lower priorized) ETL queue with additional plugins which should be runned later after all files tasks of standard priorized queue done
		# to run ETL of the file later again with additional plugins like OCR which need much time/resources while meantime all files are searchable by other plugins which need fewer resources
		if 'additional_plugins_later' in parameters:
			index_file.apply_async( kwargs={ 'filename': parameters['filename'], 'additional_plugins': parameters['additional_plugins_later'] }, queue='tasks', priority=1 )
			
		# Since wont be processed further by this ETL run / no export to index in this ETL run but by new ETL worker from queue, we added this file in this plugin
		parameters['break'] = True
	
		return parameters, data
