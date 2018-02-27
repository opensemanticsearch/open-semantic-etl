#!/usr/bin/python3
# -*- coding: utf-8 -*-

#
# Import annotations from Hypothesis - https://hypothes.is
#

# Todo: Paging, so not only import maximum last 200 annotations, which might be all since last run, but not if rebuild index and > 200 annotations per queried user or queried document

import requests
import json
import sys

from etl import ETL

from etl_web import Connector_Web

import export_solr

class Connector_Hypothesis(ETL):

	verbose = False

	documents = True
	
	token = None
	
	api = 'https://hypothes.is/api/'
	
	# initialize Open Semantic ETL
	etl = ETL()
	etl.read_configfile ('/etc/etl/config')
	etl.read_configfile ('/etc/opensemanticsearch/etl')
	etl.read_configfile ('/etc/opensemanticsearch/hypothesis')
	etl.verbose = verbose

	exporter = export_solr.export_solr()


	#
	# index the annotated document, if not yet in index
	#
	
	def etl_document(self, uri):
	
		result = True
		doc_mtime = self.exporter.get_lastmodified(docid=uri)
	
		if doc_mtime:
	
			if self.verbose:
				print ("Annotated document in search index. No new indexing of {}".format(uri))
	
		else:
			# Download and Index the new or updated uri
	
			if self.verbose:
				print ("Annotated document not in search index. Start indexing of {}".format(uri))
		
			try:
				etl = Connector_Web()
				etl.index(uri=uri)
			except KeyboardInterrupt:
				raise KeyboardInterrupt	
			except BaseException as e:
				sys.stderr.write( "Exception while getting {} : {}".format(uri, e) )
				result = False
		return result


	#
	# import an annotation
	#
	
	def etl_annotation(self, annotation):	

		parameters = {}
		parameters['plugins'] = []
	
		# since there can be multiple annotations for same URI,
		# do not overwrite but add value to existent values of the facet/field/property
		parameters['add'] = True
		data = {}

		# id/uri of the annotated document, not the annotation id
		parameters['id'] = annotation['uri']

		# first index / etl the webpage / document that has been annotated if not yet in index
		if self.documents:
			result = self.etl_document(uri=annotation['uri'])
		if not result:
			data['etl_error_hypothesis_ss'] = "Error while indexing the document that has been annotated"

		# annotation id
		data['annotation_id_ss'] = annotation['id']

		data['annotation_text_tt'] = annotation['text']

		tags = []
		if 'tags' in annotation:

			if self.verbose:
				print ( "Tags: {}".format(annotation['tags']) )

			for tag in annotation['tags']:
				tags.append(tag)
		data['annotation_tag_ss'] = tags


		# write annotation to database or index
		self.etl.process(parameters=parameters, data=data)

	
	#
	# import all annotations since last imported annotation
	#
	
	def etl_annotations(self, last_update="", user=None, group=None, tag=None, uri=None):

		limit = 200

		newest_update = last_update
	
		if not self.api.endswith('/'):
			self.api = self.api + '/'
		
		searchurl = '{}search?limit={}&sort=updated&order=desc'.format(self.api, limit)

		if user:
			searchurl += "&user={}".format(user)

		if group:
			searchurl += "&group={}".format(group)

		if tag:
			searchurl += "&tag={}".format(tag)

		if uri:
			searchurl += "&uri={}".format(hypothesis.uri)


		# Authorization	
		headers = {'user-agent': 'Open Semantic Search'}
		
		if self.token:
			headers['Authorization'] = 'Bearer ' + self.token

		# stats
		stat_downloaded_annotations = 0
		stat_imported_annotations = 0
	
		# Call API / download annotations
		if self.verbose:
			print ( "Calling hypothesis API {}".format(searchurl) )
	
		request = requests.get(searchurl, headers=headers)
	
		result = json.loads(request.content.decode('utf-8'))
	
		# import annotations
		for annotation in result['rows']:
	
			stat_downloaded_annotations += 1
	
			if annotation['updated'] > last_update:
				
				if self.verbose:
					print ( "Importing new annotation {}annotations/{}".format(self.api, annotation['id']) )
					print (annotation['text'])
				
				stat_imported_annotations += 1
	
				# save update time from newest annotation/edit
				if annotation['updated'] > newest_update:
					newest_update = annotation['updated']
		
				self.etl_annotation(annotation)	

		# commit to index, if yet buffered
		self.etl.commit()
	
		if self.verbose:
			print ("Downloaded annotations: {}".format(stat_downloaded_annotations))
			print ("Imported new annotations: {}".format(stat_imported_annotations))
	
		return newest_update


#
# Read command line arguments and start
#

#if running (not imported to use its functions), run main function
if __name__ == "__main__":

	from optparse import OptionParser 

	#get uri or filename from args

	parser = OptionParser("etl-file [options] filename")
	parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=None, help="Print debug messages")
	parser.add_option("-a", "--api", dest="api", default="https://hypothes.is/api/", help="API URL")
	parser.add_option("-p", "--token", dest="token", default=None, help="API token for authorization")
	parser.add_option("-d", "--documents", dest="documents", action="store_true", default=True, help="Index content of annotated document(s), too")
	parser.add_option("-f", "--force", dest="force", action="store_true", default=None, help="Force (re)indexing, even if no changes")
	parser.add_option("-c", "--config", dest="config", default=False, help="Config file")
	parser.add_option("-t", "--tag", dest="tag", default=None, help="Filter for a tag")
	parser.add_option("-u", "--user", dest="user", default=None, help="Filter for an user")
	parser.add_option("-g", "--group", dest="group", default=None, help="Filter for a group")

	(options, args) = parser.parse_args()

	connector = Connector_Hypothesis()

	# add optional config parameters
	if options.config:
		connector.read_configfile(options.config)

	if options.verbose == False or options.verbose==True:
		connector.verbose = options.verbose
		
	connector.documents = options.documents

	if options.token:
		connector.token = options.token

	connector.api = options.api

	connector.etl_annotations(last_update="", user=options.user, group=options.group, tag=options.tag)
