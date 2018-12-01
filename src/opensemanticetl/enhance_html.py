#
# Extracts text within configured HTML tags / XML tags
#

import os.path
import sys

from lxml import etree


class enhance_html(object):

	def elements2data(self, element, data, path=None, recursive=True):

		if self.verbose:
			print ("Extracting element {}".format(element.tag))

		if path:
			path += "/" + element.tag
		else:
			path = element.tag

		fieldname = path + '_ss'
		
		text = element.text

		if text:
			text = text.strip()
	
		if text:
			if fieldname in data:
				data[fieldname].append( text )
			else:
				data[fieldname] = [ text ]

		if recursive:
			for child in element:
				data = self.elements2data(element=child, path = path, data = data, recursive=True)

		return data


	def process (self, parameters={}, data={} ):
	
		self.verbose = False
		if 'verbose' in parameters:
			if parameters['verbose']:	
				self.verbose = True
	
		filename = parameters['filename']
	
		if 'content_type_ss' in data:
			mimetype = data['content_type_ss']
		else:
			mimetype = parameters['content_type_ss']

		#if connector returns a list, use only first value (which is the only entry of the list)
		if isinstance(mimetype, list):
			mimetype = mimetype[0]
		
		if mimetype.startswith('application/xhtml+xml'):
		
			html_extract_tags = []
			if 'html_extract_tags' in parameters:
				html_extract_tags = parameters['html_extract_tags']
	
			html_extract_tags_and_children = []
			if 'html_extract_tags_and_children' in parameters:
				html_extract_tags_and_children = parameters['html_extract_tags_and_children']
	
			parser = etree.HTMLParser()
	
			et = etree.parse(filename, parser)
	
			for xpath in html_extract_tags:
				for el in et.xpath(xpath):
					self.elements2data(element=el, data=data, recursive=False)
	
			for xpath in html_extract_tags_and_children:
				for el in et.xpath(xpath):
					self.elements2data(element=el, data=data)
		
		return parameters, data
