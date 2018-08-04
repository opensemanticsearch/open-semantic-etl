import xml.etree.ElementTree as ElementTree
import os.path
import sys


#
# is there a xmp sidecar file?
#

def get_xml_filename(filename):
	
	xmlfilename = False

	# some xmp sidecar filenames are based on the original filename without extensions like .jpg or .jpeg
	filenamewithoutextension = '.' . join( filename.split('.')[:-1] )
	
	# check if a xmp sidecar file exists
	if os.path.isfile(filename + ".xml"):
		xmlfilename = filename + ".xml"
	elif os.path.isfile(filename + ".XML"):
		xmlfilename = filename + ".XML"
	elif os.path.isfile(filenamewithoutextension + ".xml"):
		xmlfilename = filenamewithoutextension + ".xml"
	elif os.path.isfile(filenamewithoutextension + ".XML"):
		xmlfilename = filenamewithoutextension + ".XML"

	return xmlfilename








class enhance_xml(object):

	def elements2data(self, element, data, path="xml"):

		path += "/" + element.tag

		fieldname = path + '_ss'
		
		text = element.text.strip()
	
		if text:
			if fieldname in data:
				data[fieldname].append( text )
			else:
				data[fieldname] = [ text ]
					
		for child in element:
			data = self.elements2data(element=child, path=path, data = data)

		return data


	def process (self, parameters={}, data={} ):
	
		verbose = False
		if 'verbose' in parameters:
			if parameters['verbose']:	
				verbose = True
	
		filename = parameters['filename']
	
		
		#
		# is there a xmp sidecar file?
		#
		xmlfilename = get_xml_filename(filename)
		
		if not xmlfilename:
			if verbose:
				print ("No xml sidecar file")
		
		#
		# read meta data of the xmp sidecar file (= xml + rdf)
		#
		if xmlfilename:
			
			
			if verbose:
				print ("Reading XML sidecar file ".format(xmpfilename) )
			try:
				
				# Parse the XML file
				parser = ElementTree.XMLParser()
				et = ElementTree.parse(xmlfilename, parser)
				root = et.getroot()

				for child in root:
					self.elements2data(element=child, path=root.tag, data=data)
	
			except BaseException as e:
				sys.stderr.write( "Exception while parsing XML {} {}".format(xmlfilename, e) )
		
	
		return parameters, data
