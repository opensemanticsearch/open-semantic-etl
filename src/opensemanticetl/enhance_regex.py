#!/usr/bin/python3
# -*- coding: utf-8 -*-

import re
import etl_plugin_core


def regex2facet(data, text, regex, group, facet, verbose=False):

    if verbose:
        print("Checking regex {} for facet {}".format(regex, facet))

    matches = re.finditer(regex, text, re.IGNORECASE)

    if matches:
        for match in matches:

            try:
                value = match.group(group)
                if verbose:
                    print("Found regex {} with value {} for facet {}".format(
                        regex, value, facet))

                etl_plugin_core.append(data, facet, value)

            except BaseException as e:
                print("Exception while adding value {} from regex {} and group {} to facet {}:".format(
                    value, regex, group, facet))
                print(e.args[0])


# opens a tab with regexes and facets
def readregexesfromfile(data, text, filename, verbose=False):
    listfile = open(filename)

    # search all the lines
    for line in listfile:
        try:
            line = line.strip()

            # ignore empty lines and comment lines (starting with #)
            if line and not line.startswith("#"):
                facet = 'tag_ss'
                columns = line.split("\t")

                regex = columns[0]

                if len(columns) > 1:
                    facet = columns[1]

                if len(columns) > 2:
                    group = int(columns[2])
                else:
                    group = 0

                regex2facet(data=data, text=text, regex=regex,
                            group=group, facet=facet, verbose=verbose)

        except BaseException as e:
            print("Exception while checking line {} of regexlist {}:".format(
                line, filename))
            print(e.args[0])

    listfile.close()


#
# add to configured facet, if entry in list is in text
#

class enhance_regex(object):
    def process(self, parameters=None, data=None):

        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        regexlists = {}

        if 'regex_lists' in parameters:
            regexlists = parameters['regex_lists']

        # collect/copy to be analyzed text from all fields
        text = etl_plugin_core.get_text(data=data)

        for regexlistfile in regexlists:

            try:

                readregexesfromfile(data=data, text=text,
                                    filename=regexlistfile, verbose=verbose)

            except BaseException as e:
                print("Exception while checking regex list {}:".format(regexlistfile))
                print(e.args[0])

        return parameters, data
