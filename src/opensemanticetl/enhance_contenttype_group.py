#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# Map/aggregate content type to content type group
#


class enhance_contenttype_group(object):

    fieldname = 'content_type_group_ss'

    contenttype_groups = {
        'application/vnd.ms-excel': 'Spreadsheet',
        'application/vnd.oasis.opendocument.spreadsheet': 'Spreadsheet',
        'application/vnd.oasis.opendocument.spreadsheet-template': 'Spreadseheet template',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'Spreadsheet',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.template': 'Spreadsheet template',
        'text': 'Text document',
        'application/gzip text': 'Text document',
        'application/pdf': 'Text document',
        'application/msword': 'Text document',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'Text document',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.template': 'Text document template',
        'application/vnd.oasis.opendocument.text': 'Text document',
        'application/vnd.oasis.opendocument.text-template': 'Text document template',
        'application/rtf': 'Text document',
        'application/vnd.ms-powerpoint': 'Presentation',
        'application/vnd.oasis.opendocument.presentation': 'Presentation',
        'application/vnd.oasis.opendocument.presentation-template': 'Presentation template',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'Presentation',
        'application/vnd.openxmlformats-officedocument.presentationml.template': 'Presentation template',
        'image': 'Image',
        'audio': 'Audio',
        'video': 'Video',
        'application/mp4': 'Video',
        'application/x-matroska': 'Video',
        'application/vnd.etsi.asic-e+zip': 'Electronic Signature Container',
        'Knowledge graph': 'Knowledge graph',
    }

    suffix_groups = {
        '.csv': "Spreadsheet",
    }

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        content_types = []
        if 'content_type_ss' in data:
            content_types = data['content_type_ss']

        if not isinstance(content_types, list):
            content_types = [content_types]

        groups = []

        for content_type in content_types:

            # Contenttype to group
            for mapped_content_type, group in self.contenttype_groups.items():
                if content_type.startswith(mapped_content_type):
                    if not group in groups:
                        groups.append(group)

            # Suffix to group
            for suffix, group in self.suffix_groups.items():
                if parameters['id'].upper().endswith(suffix.upper()):
                    if not group in groups:
                        groups.append(group)

            if len(groups) > 0:
                data[self.fieldname] = groups

        return parameters, data
