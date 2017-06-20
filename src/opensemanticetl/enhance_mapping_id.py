#!/usr/bin/python
# -*- coding: utf-8 -*-


#
# Map paths or domains
#

class enhance_mapping_id(object):

	def process (self, parameters={}, data={} ):
				
		if 'mappings' in parameters:
			parameters['id'] = mapping( value = parameters['id'], mappings = parameters['mappings'] )
	
		return parameters, data


# Change value with best/deepest mapping
def mapping( value, mappings={} ):

	best_match_count = -1

	# check all mappings for matching and use the best
	for map_from, map_to in mappings.items():

		# map from matching value?
		if value.startswith(map_from):

			# if from string longer (deeper path), this is the better matching
			match_count = len(map_from)

			if match_count > best_match_count:
				best_match_count = match_count
				best_match_map_from = map_from
				best_match_map_to = map_to

	# if there is a match, replace first occurance of value with mapping
	if best_match_count >= 0:
		value = value.replace(best_match_map_from, best_match_map_to, 1)

	return value
