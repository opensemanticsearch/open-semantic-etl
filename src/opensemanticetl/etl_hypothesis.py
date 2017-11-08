#!/usr/bin/python3
# -*- coding: utf-8 -*-

#
# Import annotations from Hypothesis - https://hypothes.is
#

# Todo: Paging, so not only import maximum last 200 annotations, which might be all since last run, but not if rebuild index and > 200 annotations per queried user or queried document


import requests
import json

from etl import ETL


verbose = True


def etl_hypothesis_annotations(searchurl, last_update=""):

	etl = ETL()
	etl.read_configfile ('/etc/etl/config')
	etl.read_configfile ('/etc/opensemanticsearch/etl')
	etl.read_configfile ('/etc/opensemanticsearch/hypothesis')
	etl.verbose = verbose
	
	

	if verbose:
		print ( "Get from hypothesis API {}".format(searchurl) )
	request = requests.get(searchurl)

	result = json.loads(request.content.decode('utf-8'))

	parameters = {}
	parameters['plugins'] = []

	# since there can be multiple annotations for same URI,
	# do not overwrite but add value to existent values of the facet/field/property
	parameters['add'] = True
	newest_update = last_update

	stat_downloaded_annotations = 0
	stat_imported_annotations = 0

	for annotation in result['rows']:

		stat_downloaded_annotations += 1

		if annotation['updated'] > last_update:
			
			stat_imported_annotations += 1

			# save update time from newest annotation/edit
			if annotation['updated'] > newest_update:
				newest_update = annotation['updated']
	
			data = {}
	
			# id/uri of the annotated document, not the annotation id
			parameters['id'] = annotation['uri']

			# annotation id
			data['annotation_id_ss'] = annotation['id']

			data['annotation_text_tt'] = annotation['text']

			tags = []
			if 'tags' in annotation:
				for tag in annotation['tags']:
					tags.append(tag)
			data['annotation_tag_ss'] = tags


			# write annotation to database or index
			etl.process(parameters=parameters, data=data)

	etl.commit()

	if verbose:
		print ("Downloaded annotations: {}".format(stat_downloaded_annotations))
		print ("Imported new annotations: {}".format(stat_imported_annotations))


	return newest_update

