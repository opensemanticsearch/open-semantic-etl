from elasticsearch import Elasticsearch


# Connect to Elastic Search

class export_elasticsearch(object):
	
	def __init__(self, config = {} ):
		
		self.config = config

		if not 'index' in self.config:
			self.config['index'] = 'opensemanticsearch'
		
		if not 'verbose' in self.config:
			self.config['verbose'] = False


	#
	# Write data to Elastic Search
	#

	def process (self, parameters={}, data={} ):

		self.config = parameters
		
		# post data
		self.update(parameters=parameters, data=data)
	
		return parameters, data


	# send the updated field data to Elastic Search
	def update(self, docid=None, data={}, parameters={}):
	
		if docid:
			parameters['id'] = docid
		else:
			docid = parameters['id']
		
		es = Elasticsearch()
		result = es.index(index=self.config['index'], doc_type='document', id=docid, body=data)

		return result
	
	
	# get last modified date for document
	def get_lastmodified(self, docid, parameters = {}):
	
		es = Elasticsearch()

		doc_exists = es.exists(index=self.config['index'], doc_type="document", id=docid)

		# if doc with id exists in index, read modification date
		if doc_exists:	
			doc = es.get(index=self.config['index'], doc_type="document", id=docid, _source=False, fields="file_modified_dt")
			last_modified = doc['fields']['file_modified_dt'][0]
		else:
			last_modified=None
			
		return last_modified


	# commits are managed by Elastic Search setup, so no explicit commit here
	def commit(self):
		return
	