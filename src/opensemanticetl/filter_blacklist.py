#!/usr/bin/python
# -*- coding: utf-8 -*-

import re


def is_in_lists(listfiles, value, match=None):

    result = False

    for listfile in listfiles:

        try:
            if is_in_list(filename=listfile, value=value, match=match):
                result = True
                break

        except BaseException as e:
            print("Exception while checking blacklist {}:".format(listfile))
            print(e.args[0])

    return result


#
# is a value in a textfile with a list
#
def is_in_list(filename, value, match=None):

    result = False
    listfile = open(filename)

    # search all the lines
    for line in listfile:
        line = line.strip()

        # ignore empty lines and comment lines (starting with #)
        if line and not line.startswith("#"):

            if match == 'prefix':
                if value.startswith(line):
                    result = True
            elif match == 'suffix':
                if value.endswith(line):
                    result = True
            elif match == 'regex':
                if re.search(line, value):
                    result = True

            else:
                if line == value:
                    result = True

            if result:

                # we dont have to check rest of list
                break

    listfile.close()

    return result


#
# add to configured facet, if entry in list is in text
#

class filter_blacklist(object):

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        blacklisted = False

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        uri = parameters['id']

        # if blacklist type configurated in parameters, check this blacklists for URI

        if 'blacklist_prefix' in parameters:

            if is_in_lists(listfiles=parameters['blacklist_prefix'], value=uri, match="prefix"):
                blacklisted = True

        if not blacklisted and 'blacklist_suffix' in parameters:

            if is_in_lists(listfiles=parameters['blacklist_suffix'], value=uri, match="suffix"):
                blacklisted = True

        if not blacklisted and 'blacklist_regex' in parameters:

            if is_in_lists(listfiles=parameters['blacklist_regex'], value=uri, match="regex"):
                blacklisted = True

        if not blacklisted and 'blacklist' in parameters:

            if is_in_lists(listfiles=parameters['blacklist'], value=uri):
                blacklisted = True

        # check whitelists for URI, if blacklisted

        if blacklisted and 'whitelist_prefix' in parameters:
            if is_in_lists(listfiles=parameters['whitelist_prefix'], value=uri, match="prefix"):
                blacklisted = False

        if blacklisted and 'whitelist_suffix' in parameters:
            if is_in_lists(listfiles=parameters['whitelist_suffix'], value=uri, match="suffix"):
                blacklisted = False

        if blacklisted and 'whitelist_regex' in parameters:
            if is_in_lists(listfiles=parameters['whitelist_regex'], value=uri, match="regex"):
                blacklisted = False

        if blacklisted and 'whitelist' in parameters:
            if is_in_lists(listfiles=parameters['whitelist'], value=uri):
                blacklisted = False

        # if blacklisted and not matched whitelist, return parameter break, so no further processing
        if blacklisted:
            parameters['break'] = True

        return parameters, data
