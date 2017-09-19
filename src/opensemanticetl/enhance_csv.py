import sys
import os
import csv
import urllib.request
from etl import ETL


# import each row of CSV file to index
# write CSV cols to database columns or facets


class enhance_csv(object):

	def __init__(self, verbose=False):

		self.verbose = verbose

		self.config = {}
		self.titles = False
		self.cache = False

		self.encoding = 'utf-8'
		
		self.delimiter = None

		self.start_row = 1

		self.title_row = 0

		self.cols=[]
		self.rows=[]
		self.cols_include = False
		self.rows_include = False

		self.sniff_dialect = True

		self.quotechar = None
		self.doublequote = None
		self.escapechar = None


	def read_parameters(self, parameters, data):
			
		if 'verbose' in parameters:
			if parameters['verbose']:
				self.verbose = True

		if 'encoding' in parameters:
			self.encoding = parameters['encoding']
		elif 'encoding_s' in data:
			self.encoding = data['encoding_s']

		if 'delimiter' in parameters:
			self.delimiter = parameters['delimiter']

		if 'cache' in parameters:
			self.cache = parameters['cache']

		if 'title_row' in parameters:
			self.title_row = parameters['title_row']
	
		if 'start_row' in parameters:
			self.start_row = parameters['start_row']

		if 'sniff_dialect' in parameters:
			self.sniff_dialect = parameters['sniff_dialect']
	
		if 'quotechar' in parameters:
			self.quotechar = parameters['quotechar']
			
		if 'doublequote' in parameters:
			self.doublequote = parameters['doublequote']
	
		if 'escapechar' in parameters:
			self.escapechar = parameters['escapechar']

		if 'rows' in parameters:
			self.rows = parameters['rows']

		if 'cols' in parameters:
			self.cols = parameters['cols']

		if 'rows_include' in parameters:
			self.rows_include = parameters['rows_include']

		if 'cols_include' in parameters:
			self.cols_include = parameters['cols_include']
	

	# Todo:

	#
	# If existing CSV parameter settings in CSV manager, use them
	# even if not importing within CSV manager
	#
	def add_csv_parameters_from_meta_settings(self, metaserver):
		pass
			# get csv settings for this file from csvmnager
			#json = get csvserver

			#if delimiter in json:
			#	parameters['delimiter'] = json['delimiters']
	


	#
	# Build CSV dialect
	#
	
	# Autodetect and/or construct from parameters
	def get_csv_dialect(self):

		kwargs={}
	
		# automatically detect dialect
		sniffed_dialect = False
	
		if self.sniff_dialect:
			try:
				if self.verbose:
					print ( "Opening {} for guessing CSV dialect".format(self.filename) )	

				csvfile = open(self.filename, newline='', encoding=self.encoding)
		
				if self.verbose:
					print ("Starting dialect guessing")
		
				# sniff dialect in first 32 MB
				sniffsize = 33554432
				sniffed_dialect = csv.Sniffer().sniff(csvfile.read(sniffsize))
			
				if self.verbose:
					print ( "Sniffed dialect: {}".format(sniffed_dialect) )
		
			except KeyboardInterrupt:
				raise KeyboardInterrupt

			except BaseException as e:
				sys.stderr.write( "Exception while CSV format autodetection for {}: {}".format(self.filename, e) )
	
			finally:
				csvfile.close()
	
	
		if sniffed_dialect:
			kwargs['dialect'] = sniffed_dialect
		else:
			kwargs['dialect'] = 'excel'
		
		# Overwrite options, if set
		if self.delimiter:
			kwargs['delimiter'] = str(self.delimiter)
	
		if self.quotechar:
			kwargs['quotechar'] = str(self.quotechar)
	
		if self.escapechar:
			kwargs['escapechar'] = str(self.escapechar)
	
		if self.doublequote:
			kwargs['doublequote'] = self.doublequote
		
	
		return kwargs


	
	def set_titles(self, row):
	
		self.titles=[]
		colnumber = 0

		for col in row:
	
			colnumber += 1
		
			self.titles.append(col)
	
		return self.titles

	
	def export_row_data_to_index(self, data, rownumber):

		parameters = self.config.copy()
		
		# todo: all content plugins configurated, not only this one
		parameters['plugins'] = [
			'enhance_path',
		]

		etl = ETL()

		try:
			
			etl.process( parameters=parameters, data=data)
		
		# if exception because user interrupted by keyboard, respect this and abbort		
		except KeyboardInterrupt:
			raise KeyboardInterrupt
		except BaseException as e:
			sys.stderr.write( "Exception adding CSV row {} : {}".format(rownumber, e) )

			if 'raise_pluginexception' in self.config:
				if self.config['raise_pluginexception']:
					raise e
	
	
	def import_row(self, row, rownumber, docid):
	
		colnumber = 0
		
		data = {}
	
		data['content_type'] = "CSV row"
		
		data['container_s'] = docid
	
		data['page_i'] = str(rownumber)
	
		data['id'] = docid + '#' + str(rownumber)
	
		for col in row:
	
			colnumber += 1

			exclude_column = False

			if self.cols_include:
				if not colnumber in self.cols:
					exclude_column = True
			else:
				if colnumber in self.cols:
					exclude_column = True


			if not exclude_column:
				
				if self.titles and len(self.titles) >= colnumber:
					fieldname = self.titles[colnumber-1] + "_t"
				else:
					fieldname = 'column_' + str(colnumber).zfill(2) + "_t"
					
				data[fieldname] = col
				
				
				# if number, save as float value, too
				try:
					if self.titles and len(self.titles) >= colnumber:
						fieldname = self.titles[colnumber-1] + "_f"
					else:
						fieldname = 'column_' + str(colnumber).zfill(2) + "_f"
					data[fieldname] = float(col)
				except ValueError:
					pass

				self.export_row_data_to_index (data=data, rownumber=rownumber)

		return colnumber


	


	#
	# read parameters, analyze csv dialect and import row by row
	#
	
	def enhance_csv(self, parameters, data):

		self.config = parameters.copy()

		docid = parameters['id']

		#
		# Read parameters
		#

		self.read_parameters(parameters, data)
		
		if 'csvmanager' in parameters:
			self.read_csv_parameters_from_meta_settings(metaserver=parameters['csvmanager'], docid=docid)

	
		# Download, if not a file(name) yet but URI reference
		
		# todo: move to csv manager or downloader plugin that in that case should use etl_web
		if 'filename' in parameters:

			is_tempfile = False

			self.filename = parameters['filename']

			# if exist delete protocoll prefix file://
			if self.filename.startswith("file://"):
				self.filename = self.filename.replace("file://", '', 1)

		else:

			# Download url to an tempfile
			is_tempfile = True
			self.filename, headers = urllib.request.urlretrieve(self.filename)

			
		#
		# Get CSV dialect parameters
		#
		
		dialect_kwargs = self.get_csv_dialect()
	
	
		if self.verbose:
			print ( "Opening CSV file with Encoding {} and dialect {}".format( self.encoding, dialect_kwargs) )

		#
		# Open and read CSV
		#
		
		csvfile = open(self.filename, newline='', encoding=self.encoding)
	
		reader = csv.reader(csvfile, **dialect_kwargs)
		
		# increase limits to maximum, since there are often text fields with longer texts
		csv.field_size_limit(sys.maxsize)
		
	
		rownumber = 0
	
	
		#
		# Read CSV row by row
		#
	
		for row in reader:

			rownumber += 1
	
			#
			# If title row, read column titles
			#
			if rownumber == self.title_row:
	
				if self.verbose:
					print ("Importing Titles from row {}".format(self.title_row))
	
				self.set_titles(row)
	
			#
			# Import data row
			#
			if rownumber >= self.start_row:



				exclude_row = False

				if self.rows_include:
					if not rownumber in self.rows:
						exclude_row = True
				else:
					if rownumber in self.rows:
						exclude_row = True


				if exclude_row:
					if self.verbose:
						print ( "Excluding row {}".format(rownumber) )
				else:
					
					if self.verbose:
						print ( "Importing row {}".format(rownumber) )
	
					count_columns = self.import_row(row, rownumber = rownumber, docid = docid)
					
		
		#
		# delete if downloaded tempfile
		#
		if not self.cache:
			if is_tempfile:
				os.remove(self.filename)
	
	
		#
		# Print stats
		#
	
		if self.verbose:
			print ( "Rows: " + str(rownumber) )
			print ( "Cols: " + str(count_columns) )
		
		return rownumber



	def process (self, parameters={}, data={} ):

		docid = parameters['id']

		# if CSV (file suffix is .csv), enhance it (import row by row)
		if docid.lower().endswith('.csv') or docid.lower().endswith('.tsv') or docid.lower().endswith('.tab'):
			self.enhance_csv(parameters, data)

		return parameters, data
