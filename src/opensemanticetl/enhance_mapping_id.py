#!/usr/bin/python
# -*- coding: utf-8 -*-


#
# Map paths or domains
#

class enhance_mapping_id(object):

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        if 'mappings' in parameters:
            parameters['id'] = mapping(
                value=parameters['id'], mappings=parameters['mappings'])

        return parameters, data


# Change value with best/deepest mapping
def mapping(value, mappings=None):
    if mapping is None:
        mappings = {}

    max_match_len = -1

    # check all mappings for matching and use the best
    for map_from, map_to in mappings.items():

        # map from matching value?
        if value.startswith(map_from):

            # if from string longer (deeper path), this is the better matching
            match_len = len(map_from)

            if match_len > max_match_len:
                max_match_len = match_len
                best_match_map_from = map_from
                best_match_map_to = map_to

    # if there is a match, replace first occurance of value with mapping
    if max_match_len >= 0:
        value = value.replace(best_match_map_from, best_match_map_to, 1)

    return value


# Change mapped value to origin value
def mapping_reverse(value, mappings=None):
    if mapping is None:
        mappings = {}

    max_match_len = -1

    # check all mappings for matching and use the best
    for map_from, map_to in mappings.items():

        # map from matching value?
        if value.startswith(map_to):

            # if from string longer (deeper path), this is the better matching
            match_len = len(map_to)

            if match_len > max_match_len:
                max_match_len = match_len
                best_match_map_from = map_from
                best_match_map_to = map_to

    # if there is a match, replace first occurance of value with reverse mapping
    if max_match_len >= 0:
        value = value.replace(best_match_map_to, best_match_map_from, 1)

    return value
