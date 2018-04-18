#!/usr/bin/python3
# -*- coding: utf-8 -*-

import requests
import json
import urllib.request
import urllib.parse

# Export data to Solr
class export_solr(object):
	
	def __init__(self, solr = 'http://localhost:8983/solr/', core = 'core1', verbose = False):
	
		self.verbose = verbose
	
		self.solr = solr
		self.core = core


	#
	# Write data to Solr
	#

	def process (self, parameters = {}, data = {} ):

		# if not there, set config defaults
		if 'verbose' in parameters:
			self.verbose = parameters['verbose']
	
		if 'solr' in parameters:
			self.solr = parameters['solr']
			if not self.solr.endswith('/'):
				self.solr += '/'

		if 'index' in parameters:
			self.core = parameters['index']

		add = False
		if 'add' in parameters:
			add = parameters['add']
		
		fields_set = []
		if 'fields_set' in parameters:
			fields_set = parameters['fields_set']

		if not 'id' in data:
			data['id'] = parameters['id']

		# post data to Solr
		
		# do not post, if only id (which will contain no add or set commands for fields and will be seen as overwrite for whole document)
		if len(data) > 1:
			self.update(data=data, add=add, fields_set=fields_set)
	
		return parameters, data


	# update document in index, set fields in data to updated or new values or add new/additional values
	# if no document yet, it will be added
	def update(self, data, add=False, fields_set=[]):

		update_fields={}

		for fieldname in data:
			if fieldname == 'id':
				update_fields['id'] = data['id']
			else:
				update_fields[fieldname] = {}

				if add and not fieldname in fields_set:
					# add value to existent values of the field
					update_fields[fieldname]['add'] = data[fieldname]
				else:	
					# if document there with values for this fields, the existing values will be overwritten with new values
					update_fields[fieldname]['set'] = data[fieldname]

		self.post(data=update_fields)


		
	def post(self, data=[], docid=None, commit=None):
		
		solr_uri = self.solr + self.core + '/update'
	
		if docid:
			data['id'] = docid
	
		datajson = '[' + json.dumps(data) + ']'

		params = {}
		if commit:
			params['commit'] = 'true'
	
		if self.verbose:
			print ("Sending update request to {}".format(solr_uri) )
			print (datajson)

		try:

			requests.post(solr_uri, data=datajson, params=params, headers={'Content-Type': 'application/json'})

		except BaseException as e:

			import sys
			sys.stderr.write( 'Error while posting data to Solr: {}'.format(e) )
		
		
	# tag a document by adding new value to field
	def tag(self, docid = None, field = None, value = None, data = {} ):
	
		data_merged = data.copy()

		if docid:
			data_merged['id'] = docid
		
		if field:
			if field in data_merged:
				# if not list, convert to list
				if not isinstance(data_merged[field], list):
					data_merged[field] = [ data_merged[field] ]
				# add value to field
				data_merged[field].append(value)
			else:
				data_merged[field] = value
	
		result = self.update( data=data_merged, add=True )
		
		return result
	
	
	# search for documents with query and without tag and update them with the tag
	def update_by_query(self, query, field = None, value = None, data = {}, queryparameters=None):

		import pysolr
	
		count = 0
		
		solr = pysolr.Solr(self.solr + self.core)
	
	
		#
		# extend query: do not return documents, that are tagged
		#
		
		query_marked_before = ''
	
		if field:
			query_marked_before = field + ':"' + solr_mask(value) + '"'
	
		# else extract field and value from data to build query of yet tagged docs to exclude
	
		for fieldname in data:

			if isinstance(data[fieldname], list):

				for value in data[fieldname]:
		
					if query_marked_before:
						query_marked_before += " AND "
						
					query_marked_before += fieldname + ':"' + solr_mask(value) + '"'
			else:
				
				value=data[fieldname]
				if query_marked_before:
					query_marked_before += " AND "
						
				query_marked_before += fieldname + ':"' + solr_mask(value) + '"'
	
	
		solrparameters = {
			'fl': 'id',
			'defType': 'edismax',
			'rows': 10000000,
		}
	
		# add custom Solr parameters (if the same parameter, overwriting the obove defaults)
		if queryparameters:
			solrparameters.update(queryparameters)
	
	
		if query_marked_before:
			# don't extend query but use filterquery for more performance (cache) on aliases
			solrparameters["fq"] = 'NOT (' + query_marked_before + ')'
	
		if self.verbose:
			print ("Solr query:")
			print (query)
			print ("Solr parameters:")
			print (solrparameters)
	
		results = solr.search(query, **solrparameters)
		
		for result in results:
			docid = result['id']
	
			if self.verbose:
				print ( "Tagging {}".format(docid) )
			
			self.tag( docid=docid, field=field, value=value, data=data)
			
			count += 1
	
		return count


	def get_data(self, docid, fields):

		uri = self.solr + self.core + '/get?id=' + urllib.parse.quote( docid ) + '&fl=' + ','.join(fields)

		request = urllib.request.urlopen( uri )
		encoding = request.info().get_content_charset('utf-8')
		data = request.read()
		request.close()
	
		solr_doc = json.loads(data.decode(encoding))

		data = None
		if 'doc' in solr_doc:
			data = solr_doc['doc']
	
		return data

	
	def commit(self):
		
		uri = self.solr + self.core + '/update?commit=true'
		request = urllib.request.urlopen( uri )
		request.close()
	
	
	def get_lastmodified( self, docid ):
		#convert mtime to solr format
		solr_doc_mtime = None

		solr_doc = self.get_data(docid=docid, fields=["file_modified_dt"])

		if solr_doc:
			if 'file_modified_dt' in solr_doc:
				solr_doc_mtime = solr_doc['file_modified_dt']
	

			# todo: for each plugin
#			solr_meta_mtime = False
#			if 'meta_modified_dt' in solr_doc['doc']:
#				solr_meta_mtime = solr_doc['doc']['meta_modified_dt']

		return solr_doc_mtime
	

	def delete(self, parameters, docid=None, query=None,):
		import pysolr

		if 'solr' in parameters:
			self.solr = parameters['solr']
			if not self.solr.endswith('/'):
				self.solr += '/'

		if 'index' in parameters:
			self.core = parameters['index']


		solr = pysolr.Solr(self.solr + self.core)

		if docid:
			result = solr.delete(id=docid)
		
		if query:
			result = solr.delete(q=query)

		return result


def solr_mask(string_to_mask, solr_specialchars = '\+-&|!(){}[]^"~*?:/'):
	
		masked = string_to_mask
		# mask every special char with leading \
		for char in solr_specialchars:
			masked = masked.replace(char, "\\" + char)
			
		return masked
