from py2neo import Graph, Node, Relationship

#
# Export entities and connections to neo4j
#

class export_neo4j(object):
	
	verbose = False

	
	def process (self, parameters={}, data={} ):

		if 'verbose' in parameters:
			self.verbose = parameters['verbose']

		# for this facets, do not add additional entity to connect with, but write to properties of the entity
		properties = ['content_type_ss', 'content_type_group_ss', 'language_ss', 'language_s']
		
		host = 'localhost'
		if 'neo4j_host' in parameters:
			host = parameters['neo4j_host']

		user = 'neo4j'
		if 'neo4j_user' in parameters:
			user = parameters['neo4j_user']

		password = 'neo4j'
		if 'neo4j_password' in parameters:
			password = parameters['neo4j_password']
		
		graph = Graph(host=host, user=user, password=password)
		
		document_node = Node('Document', name = parameters['id'])

		if 'title' in data:
			document_node['title'] = data['title']

		# add properties from facets
		for entity_class in parameters['facets']:
			
			if entity_class in data:
				
				entity_class_label = parameters['facets'][entity_class]['label']

				if entity_class in properties:

					document_node[entity_class_label] = data[entity_class]

		graph.merge(document_node)
	
	
		# add / connect linked entities from facets
			
		for entity_class in parameters['facets']:
			
			if entity_class in data:

				entity_class_label = entity_class
				if parameters['facets'][entity_class]['label']:
					entity_class_label = parameters['facets'][entity_class]['label']

				if not entity_class in properties:
	
					relationship_label = entity_class_label
	
					if entity_class in ['person_ss','organization_ss', 'location_ss']:
						relationship_label = "Named Entity Recognition"
	
					# convert to array, if single entity / not multivalued field
					if isinstance(data[entity_class], list):
						entities = data[entity_class]
					else:
						entities = [ data[entity_class] ]
	
					for entity in entities:					
	
						if self.verbose:
							print ("Export to Neo4j: Merging entity {} of class {}".format(entity, entity_class_label))
	
						# if not yet there, add the entity to graph
						entity_node = Node(entity_class_label, name = entity)
						graph.merge(entity_node)
						
						# if not yet there, add relationship to graph
						relationship = Relationship(document_node, relationship_label, entity_node)
						graph.merge(relationship)

		
		return parameters, data
