#!/usr/bin/python
# -*- coding: utf-8 -*-

from rdflib import Graph
from rdflib import RDFS
from rdflib import Namespace

# get all labels, alternate labels and synonyms for the URI/subject s
def get_labels(g, s):

	synonyms = []

	# SKOS labels
	skos = Namespace('http://www.w3.org/2004/02/skos/core#')

	# get all RDFS labels
	for o in g.objects(s, RDFS.label):
		if not o in synonyms:
			synonyms.append(o)
			
	# append SKOS prefLabel
	for o in g.objects(s, skos['prefLabel']):
		if not o in synonyms:
			synonyms.append(o)
	
	# append SKOS altLabels
	for o in g.objects(s, skos['altLabel']):
		if not o in synonyms:
			synonyms.append(o)

	# append SKOS hiddenLabels
	for o in g.objects(s, skos['hiddenLabel']):
		if not o in synonyms:
			synonyms.append(o)

	return synonyms


#
# Adds/converts labels, alternate labels and synonyms from SKOS thesaurus to Solr synonyms config
#

def skos2solr(skos_filename, solr_synonyms_filename, append=False, narrower=True):

	#load graph
	
	g = Graph()
	
	g.parse(skos_filename)


	# open Solr synonyms file
	if append:
		target = open(solr_synonyms_filename, 'a')
	else:
		target = open(solr_synonyms_filename, 'w')

	# write config file header
	if not append:
		target.write('# Synonyms config file for Solr converted from SKOS thesaurus by SKOS to Solr' + '\n\n')
		target.write('# More infos at https://www.opensemanticsearch.org/skos2solr' + '\n')
	

	# define used ontologies / standards / properties
	skos = Namespace('http://www.w3.org/2004/02/skos/core#')
	owl = Namespace('<http://www.w3.org/2002/07/owl#')


	# since this is returing subjects more than one time ...
	#for s in g.subjects(predicate=None, object=None):

	# we use a SPARQL query with distinct to get subjects only once
	res = g.query(
    """SELECT DISTINCT ?subject
       WHERE {
          ?subject ?predicate ?object .
       }""")

	for row in res:

		# get subject from first column
		s=row[0]	

		# get all Labels for this subject
		synonyms = get_labels(g, s)
		
		# if any, write synonyms to config file
		if len(synonyms):

			#
			# URI to labels
			#
			
			# URI has synonym label (unidirectional), so even URIs can be found by its label, too
			# (not the other way, since an maybe ambigous label is not the same like an unique URI!
			target.write('\n' +  synonyms[0].encode('UTF-8') + " => " + s.encode('UTF-8') + '\n')

			#
			# Alternate labels and synonyms
			#
			
			# write all the labels to config file
			# all synonyms comma separated as an line in the plain text file
			target.write(','.join(synonyms).encode('UTF-8') + '\n')

			# linked concepts
			
			# linked other concepts or same concepts in other ontologies or thesauri
			# by SKOS:exactMatch or OWL:sameAs

			# here we only synonym the URI			
			# its labels will be defined as synonyms for the uri by the run of this subroutine for their uri, so we use URI only

			# bidirectional (seperated by comma) because both are synonyms for each other or even the same
			for o in g.objects(s, skos['exactMatch']):
				target.write('\n# SKOS:exactMatch\n')
				target.write( s.encode('UTF-8') + "," + o.encode('UTF-8') + '\n')

			for o in g.objects(s, owl['sameAs']):
				target.write('\n# OWL:sameAs\n')
				target.write('\n' + s.encode('UTF-8') + "," + o.encode('UTF-8') + '\n')

			# narower match only unidirectional (separated by =>)
			# since every truck is an automobile but not every automobile is a truck
			if narrower:
				for o in g.objects(s, skos['narrower']):
					target.write('\n# SKOS:narrower\n')
					target.write(s.encode('UTF-8') + " => " + o.encode('UTF-8') + '\n')

				for o in g.objects(s, skos['narrowMatch']):
					target.write('\n# SKOS:narrowMatch\n')
					target.write(s.encode('UTF-8') + " => " + o.encode('UTF-8') + '\n')


	target.close()



#
# Read command line arguments and convert SKOS thesaurus or RDF graph to solr synonym config
#

#if running (not imported to use its functions), run main function
if __name__ == "__main__":

	#get filenames from command line args

	from optparse import OptionParser 

	parser = OptionParser("skos2solr skos-filename solr-synonyms-filename")
	parser.add_option("-a", "--append", dest="append", action="store_true", default=False, help="Append to existing Solr synonyms file instead of overwrite it")

	(options, args) = parser.parse_args()

	if len(args) < 2:
		parser.error("No filenames given")

	# convert SKOS to Solr config
	skos2solr(skos_filename=args[0], solr_synonyms_filename=args[1], append=options.append)
