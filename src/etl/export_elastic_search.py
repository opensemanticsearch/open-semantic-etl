from elasticsearch import Elasticsearch
from elasticsearch import exceptions


# Connect to Elastic Search

class export_elastic_search(object):
	
	def __init__(self, elastic='http://localhost:9200/', commitWithin=0):
	
		self.verbose=False
	
		self.config = {}
		self.config['elastic'] = elastic
		self.config['verbose'] = self.verbose


	#
	# Write data to elastic search
	#

	def process (self, parameters={}, data={} ):

		self.config = parameters

		# if not there, set config defaults
		if 'verbose' in self.config:
			self.verbose = self.config['verbose']
	
	

		# post data to solr
		self.update(data=data)
	
		return parameters, data

	
	
	# send the updated field data to Solr
	def update(self, docid=None, data=[]):
	
		if docid:
			data['id'] = docid
		else:
			docid = data['id']
			
		es = Elasticsearch()
		result = es.index(index="opensemanticsearch", doc_type='document', id=docid, body=data)

		return result
	
	
	def get_lastmodified(self, docid, parameters = {}):
	
	
		es = Elasticsearch()

		doc_exists = es.exists(index="opensemanticsearch", doc_type="document", id=docid)

		# if doc with id exists in index, read modification date
		if doc_exists:	
			doc = es.get(index="opensemanticsearch", doc_type="document", id=docid, _source=False, fields="file_modified_dt")
			last_modified = doc['fields']['file_modified_dt'][0]
		else:
			last_modified=None
			
		return last_modified
