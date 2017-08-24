#!/usr/bin/python
# -*- coding: utf-8 -*-


#
# Map/aggregate content type to content type group
#

class enhance_contenttype_group(object):

	fieldname = 'content_type_group'

	contenttype_groups = {
		'application/vnd.ms-excel': 'Spreadsheet',
		'application/vnd.oasis.opendocument.spreadsheet': 'Spreadsheet',
		'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'Spreadsheet',
		'text/plain': 'Text document',
		'text/html': 'Text document',
		'application/pdf': 'Text document',
		'text/html': 'Text document',
		'application/msword': 'Text document',
		'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'Text document',
		'application/vnd.oasis.opendocument.text': 'Text document',
		'application/rtf': 'Text document',
		'application/vnd.ms-powerpoint': 'Presentation',
		'application/vnd.oasis.opendocument.presentation': 'Presentation',
		'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'Presentation',
		'image': 'Image',
		'audio': 'Audio',
		'video': 'Video',
		'application/mp4': 'Video',
		'application/x-matroska': 'Video',
	}

	suffix_groups = {
		'.csv': "Spreadsheet",
	}

	def process (self, parameters={}, data={} ):

		# Contenttype to group
		for contenttype, group in self.contenttype_groups.items():
			if data['content_type'].startswith(contenttype):
				data[self.fieldname] = group

		# Suffix to group
		for suffix, group in self.suffix_groups.items():
			if parameters['id'].upper().endswith(suffix.upper()):
				data[self.fieldname] = group

		return parameters, data
