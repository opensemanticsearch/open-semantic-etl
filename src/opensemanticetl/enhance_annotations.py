import os
import requests

import etl_plugin_core


# Get tags and annotations from annotation server
class enhance_annotations(etl_plugin_core.Plugin):

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        # get parameters
        docid = parameters['id']

        if os.getenv('OPEN_SEMANTIC_ETL_METADATA_SERVER'):
            server = os.getenv('OPEN_SEMANTIC_ETL_METADATA_SERVER')
        elif 'metadata_server' in parameters:
            server = parameters['metadata_server']
        else:
            server = 'http://localhost/search-apps/annotate/json'

        r = requests.get(server, params = {'uri': docid})
        r.raise_for_status()

        annotations = r.json()

        for facet in annotations:
            etl_plugin_core.append(data, facet, annotations[facet])

        return parameters, data
