#
# Write filename to Celery queue for batching and parallel processing
#

from tasks import index_file


class export_queue_files(object):

    def __init__(self, config=None):
        if config is None:
            config = {'verbose': False}
        self.config = config

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        # add file to ETL queue with standard prioritizazion
        # but don't if only plugins not runned that should be runned later (which will be added to queue in step below)
        if not 'only_additional_plugins_later' in parameters:
            index_file.apply_async(
                kwargs={'filename': parameters['filename']}, queue='tasks', priority=5)

        # add file to (lower priorized) ETL queue with additional plugins or options which should be runned later after all files tasks of standard priorized queue done
        # to run ETL of the file later again with additional plugins like OCR which need much time/resources while meantime all files are searchable by other plugins which need fewer resources
        if 'additional_plugins_later' in parameters or 'additional_plugins_later_config' in parameters:

            additional_plugins_later = []
            if 'additional_plugins_later' in parameters:
                additional_plugins_later = parameters['additional_plugins_later']

            additional_plugins_later_config = {}
            if 'additional_plugins_later_config' in parameters:
                additional_plugins_later_config = parameters['additional_plugins_later_config']

            if len(additional_plugins_later) > 0 or len(additional_plugins_later_config) > 0:

                index_file.apply_async(kwargs={
                                       'filename': parameters['filename'], 'additional_plugins': additional_plugins_later, 'config': additional_plugins_later_config}, queue='tasks', priority=1)

        return parameters, data
