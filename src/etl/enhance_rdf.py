import sys
import rdflib

from etl.etl import ETL


# Import RDF graph file granular, not only as a whole single file:
# for every entity (subject) own document with properties (predicates) as facets and its objects as values

class enhance_rdf(object):
	
	def __init__(self, verbose=False):

		self.verbose = verbose

	
	# since we want full text search we want not to use ID/URI but all labels for indexing
	def get_labels_from_rdfobject(self, obj):

		# if type not literal but URI use label(s) of this Identifier/URI instead of pure URI
		if type(obj) == rdflib.URIRef:

			values = []

			# append RDFS.label

			# get all labels for this obj
			for label in self.graph.objects(subject=obj, predicate=rdflib.RDFS.label):
				values.append( label.encode('UTF-8') )

				if self.verbose:
					print ( "Appending RDFS:label: {}".format(label.encode('UTF-8')) )

			#
			# append SKOS labels
			#
			
			# append SKOS prefLabel
			skos = rdflib.Namespace('http://www.w3.org/2004/02/skos/core#')
			for label in self.graph.objects(subject=obj, predicate=skos['prefLabel']):
				values.append( label.encode('UTF-8') )

				if self.verbose:
					print ( "Appending SKOS:prefLabel: {}".format(label.encode('UTF-8')) )

			# append SKOS altLabels
			for label in self.graph.objects(subject=obj, predicate=skos['altLabel']):
				values.append( label.encode('UTF-8') )

				if self.verbose:
					print ( "Appending SKOS:altLabel: {}".format(label.encode('UTF-8')) )

			# append SKOS hiddenLabels
			for label in self.graph.objects(subject=obj, predicate=skos['hiddenLabel']):
				values.append( label.encode('UTF-8') )

				if self.verbose:
					print ( "Appending SKOS:hiddenLabel: {}".format(label.encode('UTF-8')) )


			#if no label use URI instead
			if len(values) == 0:
			
				if self.verbose:
					print ( "No label, using URI instead: {}".format(obj) )
				values = obj

			# (maybe) todo:
			# because if not in file but available:
			# get label from external graph(s) or triplestore

		else:
			
			if self.verbose:
				print ( "Data type of value (RDF object): {}".format(type(obj)) )
				print ( "Using its value {}".format(obj) )
			
			values = obj


		return values


	def	etl_graph(self, parameters):
	
		# Print infos
		if self.verbose:
			print("Graph has {} triples.".format(len(self.graph)) )
	
		count = 0
	
		part_parameters = {}

		part_parameters['plugins'] = []
		
		# todo like enhance_path for properties & subjects?
		# abstract variable of enhance_path plugin?
		
		part_parameters['export'] = parameters['export']


		# since there can be multiple triples/values for same property,
		# do not overwrite but add value to existent values of the facet/field/property
		part_parameters['add'] = True

	
		for subj, pred, obj in self.graph:

			part_data = {}
			part_data['content_type'] = 'Knowledge graph'

			count += 1

			if self.verbose:
				print( "Importing triple {}".format(count) )

			try:					
	
				# subject as URI/ID
				part_parameters['id'] = subj

				if self.verbose:
					print ( "ID (RDF subject): {}".format(subj) )

				#
				# Predicate/property to facet/field
				#
				rdf_property = pred

				# set Solr datatype so facets not available yet in Solr schema can be inserted automatically (dynamic fields) with right datatype
				facet = rdf_property + '_ss'
	
				if self.verbose:
					print ( "Facet: {}".format(facet) )
				#
				# object to facet/field value
				#

				value = self.get_labels_from_rdfobject(obj)						
				
				# insert or append value (object of triple) to data
				part_data[facet] = value

				#
				# Property statistics
				#
				
				# add to facet property where you can see which properties are available
				part_data['property_ss'] = pred


				# todo: set parameter to add instead of update for multiple triples/values for/with same property 
	
				etl = ETL()
				
				etl.verbose = self.verbose
	

				# index triple
				etl.process( part_parameters, part_data)
	
			except KeyboardInterrupt:
				raise KeyboardInterrupt
	
			except BaseException as e:
				sys.stderr.write( "Exception while triple {}: {}\n".format(count, e) )
	

	def	etl_graph_file(self, docid, filename, parameters={}):
	
		# todo: docid 2 path
	
		self.graph = rdflib.Graph()
		self.graph.parse(filename)

		self.etl_graph(parameters=parameters)

	


	def process (self, parameters={}, data={} ):
		
		verbose = False
		if 'verbose' in parameters:
			if parameters['verbose']:	
				self.verbose = True
	
		# get parameters
		docid = parameters['id']
		filename = parameters['filename']
		
		if 'content_type' in data:
			mimetype = data['content_type']
		else:
			mimetype = parameters['content_type']

		# if connector returns a list, use only first value (which is the only entry of the list)
		if isinstance(mimetype, list):
			mimetype = mimetype[0]

		# todo: add other formats like turtle
		# if mimetype is graph, call graph import
		if mimetype.lower() == "application/rdf+xml":

			self.etl_graph_file(docid, filename, parameters=parameters)
	
		return parameters, data
