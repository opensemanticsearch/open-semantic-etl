import hashlib
import os
import sys
import tempfile
import subprocess
import codecs


# Make a text from an image
def image2text(filename, lang='eng', verbose=False, cache=None):
	
	
	#
	# Calc tempfilename
	#
	if cache:
		# calc filename from cache dir and file content not filename, which will be different temp filename in future
		md5hash = hashlib.md5(open(filename, 'rb').read().encode('utf-8')).hexdigest()
		ocr_temp_filename = cache + os.path.sep + lang +'-' + md5hash

	else:
		# calc tempfilename from temp dir and filename
		md5hash = hashlib.md5(filename.encode('utf-8')).hexdigest()
		ocr_temp_filename = tempfile.gettempdir() + os.path.sep + "opensemanticetl_ocr_" + md5hash


	# if yet in cache, dont call OCR
	if cache and os.path.isfile(ocr_temp_filename + '.txt'):

		if verbose:
			print ( 'Found in OCR cache, so reading OCR results for {} from {}'.format(filename, ocr_temp_filename) )

	# not in cache, so call Tesseract OCR
	else:
		if verbose:
			if cache:
				print ( 'Not in OCR cache, starting OCR for {}'.format(filename) )
			else:
				print ( 'Starting OCR for {}'.format(filename) )

	
		# start external OCR Program
		result = subprocess.call(['tesseract', '-l', lang , filename, ocr_temp_filename])

		if not result == 0:
			sys.stderr.write( "Error: OCR failed for {}".format(filename) )


	#
	# read text from OCR result file
	#
	
	ocr_temp_filename = ocr_temp_filename + '.txt'
	ocr_temp_file = codecs.open(ocr_temp_filename, "r", encoding="utf-8")
	ocr_txt = ocr_temp_file.read()
	ocr_temp_file.close()

	if verbose:
		print ("Characters recognized: {}".format( len(ocr_txt) ) )

	
	# delete temporary OCR result file if no cache configured
	if not cache:
		os.remove(ocr_temp_filename)
	
	
	return ocr_txt


#
# If image add ocr text
#
class enhance_ocr(object):


	# how to find uris which are not enriched yet?
	# (if not enhanced on indexing but later)

	# this plugin needs to read the field id as a parameters to enrich unenriched docs
	fields = ['id', 'content_type']
		
	# query to find documents, that were not enriched by this plugin yet
	# (since we marked documents which were OCRd with ocr_b = true
	query = "content_type: image\/* AND NOT enhance_ocr_b:true"
		
	

	def process (self, parameters={}, data={} ):
	
		verbose = False
		if 'verbose' in parameters:
			if parameters['verbose']:	
				verbose = True
	
		filename = parameters['filename']

		if 'content_type' in data:
			mimetype = data['content_type']
		else:
			mimetype = parameters['content_type']

		# if connector returns a list, use only first value (which is the only entry of the list)
		if isinstance(mimetype, list):
			mimetype = mimetype[0]
	
		if 'ocr_lang' in parameters:
			lang = parameters['ocr_lang']
		else:
			lang='eng'
	
	
		if "image" in mimetype.lower():
			if verbose:
				print ("Mimetype seems image ({}), starting OCR".format(mimetype) )
		
			ocr_txt = image2text(filename=filename, lang=lang, verbose=verbose)
			
			if ocr_txt:
				data['ocr_t'] = ocr_txt
				
			# mark the document to indicate, that it was analyzed by this plugin
			data['enhance_ocr_b'] = "true"
		
		return parameters, data

	