#
# Multilinguality
#
# Copy content language specific dynamic fields for language specific analysis like stemming, grammar or synonyms
#
# Language has been detected before by plugin enhance_detect_language using Apache Tika / OpenNLP
#

class enhance_multilingual(object):

	verbose = False

	# languages that are defined in index schema for language specific analysis and used if autodetected as documents language
	languages = ['en','fr','de','es','hu','pt','nl','ro','ru','it','cz','ar','fa']
	languages_hunspell = ['hu']

	# languages for language specific analysis even if not the autodetected document language
	languages_force = []
	languages_force_hunspell = []
	
	#
	# exclude fields like technical metadata
	#
	
	exclude_prefix = [
		'etl_',
		'AF Point',
		'Chroma ',
		'Compression ',
		'Date/Time',
		'Measured EV ',
		'Primary AF Point ',
		'Self Timer ',
		'Unknown Camera Setting ',
		'Unknown tag ',
		'White Balance',
		'access_permission:',
	]

	# suffixes of non-text fields like nubers
	exclude_suffix = ['_i', '_is', '_l', '_ls', '_b','_bs','_f', '_fs', '_d','_ds','_f','_fs','_dt','_dts']

	exclude_fields = [
		'language_s',
		'content_type_ss',
		'content_type_group_ss',
		'AEB Bracket Value_ss',
		'AE Setting_ss',
		'AF Area Height_ss',
		'AF Area Width_ss',
		'AF Area X Positions_ss',
		'AF Area Y Positions_ss',
		'AF Image Height_ss',
		'AF Image Width_ss',
		'AF Point Count_ss',
		'AF Point Selected_ss',
		'AF Points in Focus_ss',
		'Aperture Value_ss',
		'Auto Exposure Bracketing_ss',
		'Auto ISO_ss',
		'Auto Rotate_ss',
		'Base ISO_ss',
		'Bulb Duration_ss',
		'Camera Info Array_ss',
		'Camera Serial Number_ss',
		'Camera Temperature_ss',
		'Camera Type_ss',
		'Canon Model ID_ss',
		'Contrast_ss',
		'Components Configuration_ss',
		'Compressed Bits Per Pixel_ss',
		'Compression_ss',
		'Color Balance Array_ss',
		'Color Space_ss',
		'Color Temperature_ss',
		'Color Tone_ss',
		'Content-Encoding_s',
		'Continuous Drive Mode_ss',
		'Control Mode_ss',
		'Custom Functions_ss',
		'Custom Rendered_ss',
		'created_ss',
		'Creation-Date_ss',
		'Data BitsPerSample_ss',
		'Data PlanarConfiguration_ss',
		'Data Precision_ss',
		'Data SampleFormat_ss',
		'Data SignificantBitsPerSample_ss',
		'date_ss',
		'dc:format_ss',
		'dcterms:created_ss',
		'dcterms:modified_ss',
		'Dimension ImageOrientation_ss',
		'Dimension PixelAspectRatio_ss',
		'Digital Zoom_ss',
		'Display Aperture_ss',
		'Easy Shooting Mode_ss',
		'Exif Version_ss',
		'exif:DateTimeOriginal_ss',
		'exif:ExposureTime_ss',
		'exif:Flash_ss',
		'exif:FocalLength_ss',
		'exif:FNumber_ss',
		'Exif Image Height_ss',
		'Exif Image Width_ss',
		'exif:IsoSpeedRatings_ss',
		'Exposure Bias Value_ss',
		'Exposure Compensation_ss',
		'Exposure Mode_ss',
		'Exposure Time_ss',
		'F-Number_ss',
		'F Number_ss',
		'File Length_ss',
		'File Modified Date_ss',
		'File Info Array_ss',
		'File Size_ss',
		'Firmware Version_ss',
		'Flash_ss',
		'FlashPix Version_ss',
		'Flash Activity_ss',
		'Flash Details_ss',
		'Flash Exposure Compensation_ss',
		'Flash Guide Number_ss',
		'Focal Length_ss',
		'Flash Mode_ss',
		'Focal Plane Resolution Unit_ss',
		'Focal Plane X Resolution_ss',
		'Focal Plane Y Resolution_ss',
		'Focal Units per mm_ss',
		'Focus Continuous_ss',
		'Focus Distance Lower_ss',
		'Focus Distance Upper_ss',
		'Focus Mode_ss',
		'Focus Type_ss',
		'height_ss',
		'ISO Speed Ratings_ss',
		'IHDR_ss',
		'Image Height_ss',
		'Image Number_ss',
		'Image Size_ss',
		'Image Width_ss',
		'Image Type_ss',
		'Interoperability Index_ss',
		'Interoperability Version_ss',
		'Iso_ss',
		'Last-Modified_ss',
		'Last-Save-Date_ss',
		'Lens Type_ss',
		'Long Focal Length_ss',
		'Macro Mode_ss',
		'Manual Flash Output_ss',
		'Max Aperture_ss',
		'Max Aperture Value_ss',
		'Measured Color Array_ss',
		'Measured EV_ss',
		'meta:creation-date_ss',
		'meta:save-date_ss',
		'Metering Mode_ss',
		'Min Aperture_ss',
		'modified_ss',
		'ND Filter_ss',
		'Number of Components_ss',
		'Orientation_ss',
		'Optical Zoom Code_ss',
		'pdf:PDFVersion_ss',
		'pdf:docinfo:created_ss',
		'pdf:docinfo:creator_tool_ss',
		'pdf:docinfo:modified_ss',
		'pdf:docinfo:producer_ss',
		'pdf:encrypted_ss',
		'Photo Effect_ss',
		'producer_ss',
		'Record Mode_ss',
		'Related Image Height_ss',
		'Related Image Width_ss',
		'Resolution Unit_ss',
		'Saturation_ss',
		'sBIT sBIT_RGBAlpha_ss',
		'Scene Capture Type_ss',
		'Sensing Method_ss',
		'Sequence Number_ss',
		'Serial Number Format_ss',
		'Slow Shutter_ss',
		'Sharpness_ss',
		'Short Focal Length_ss',
		'Shutter Speed Value_ss',
		'Spot Metering Mode_ss',
		'SRAW Quality_ss',
		'Target Aperture_ss',
		'Target Exposure Time_ss',
		'tiff:BitsPerSample_ss',
		'tiff:ImageLength_ss',
		'tiff:ImageWidth_ss',
		'tiff:Make_ss',
		'tiff:Model_ss',
		'tiff:Orientation_ss',
		'tiff:ResolutionUnit_ss',
		'tiff:XResolution_ss',
		'tiff:YResolution_ss',
		'Thumbnail Image Valid Area_ss',
		'Thumbnail Length_ss',
		'Thumbnail Offset_ss',
		'Transparency Alpha_ss',
		'Valid AF Point Count_ss',
		'width_ss',
		'X-Parsed-By_ss',
		'X-TIKA:parse_time_millis_ss',
		'X Resolution_ss',
		'xmpTPg:NPages_ss',
		'xmp:CreatorTool_ss',
		'YCbCr Positioning_ss',
		'Y Resolution_ss',
		'Zoom Source Width_ss',
		'Zoom Target Width_ss',
	]

	exclude_fields_map = {}


	def process (self, parameters={}, data={} ):

		if 'verbose' in parameters:
			self.verbose = parameters['verbose']

		if 'languages' in parameters:
			self.languages = parameters['languages']

		if 'languages_hunspell' in parameters:
			self.languages_hunspell = parameters['languages_hunspell']

		if 'languages_force' in parameters:
			self.languages_force = parameters['languages_force']

		if 'languages_force_hunspell' in parameters:
			self.languages_force_hunspell = parameters['languages_force_hunspell']

		if 'languages_exclude_fields' in parameters:
			self.exclude_fields = parameters['languages_exclude_fields']

		if 'languages_exclude_fields_map' in parameters:
			self.exclude_fields_map = parameters['languages_exclude_fields_map']

		language = None
		if "language_s" in data:
			language = data['language_s']

		language_fields = ['_text_']
		language_specific_data = {}


		# language specific analysis for recognized language of document
		# if language support of detected language in index schema
		if language in self.languages:
			language_fields.append( "text_txt_" + language )
					
		if language in self.languages_hunspell:
			language_fields.append( "text_txt_hunspell_" + language )


		# fields for language specific analysis by forced languages even if other language or false recognized language
		for language_force in self.languages_force:
			
			language_field = "text_txt_" + language_force
			
			if not language_field in language_fields:
				language_fields.append( language_field )
				
		for language_force in self.languages_force_hunspell:
			
			language_field = "text_txt_hunspell_" + language_force
			
			if not language_field in language_fields:
				language_fields.append( language_field )


		# copy each data field to language specific field with suffix _txt_$language
		for fieldname in data:

			exclude = False

			# do not copy excluded fields
			for exclude_field in self.exclude_fields:
				if fieldname == exclude_field:
					exclude = True

			for prefix in self.exclude_prefix:
				if fieldname.startswith(prefix):
					exclude = True

			for suffix in self.exclude_suffix:
				if fieldname.endswith(suffix):
					exclude = True

			if not exclude and data[fieldname]:

				# copy field to default field with added suffixes for language dependent stemming/analysis
				for language_field in language_fields:
					
					excluded_by_mapping = False
					
					if language_field in self.exclude_fields_map:
						if fieldname in self.exclude_fields_map[language_field]:
							excluded_by_mapping = True
							if self.verbose:
								print ( "Multilinguality: Excluding field {} to be copied to {} by config of exclude_field_map".format(fieldname, language_field) )

					if not excluded_by_mapping:
						if self.verbose:
							print ( "Multilinguality: Add {} to {}".format(fieldname, language_field) )
							
						if not language_field in language_specific_data:
							language_specific_data[language_field] = []
	
						if isinstance(data[fieldname], list):
							language_specific_data[language_field].extend(data[fieldname])
						else:
							language_specific_data[language_field].append(data[fieldname])
	

		# append language specific fields to data
		for key in language_specific_data:
			data[key] = language_specific_data[key]
		
		return parameters, data
