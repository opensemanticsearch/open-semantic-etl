#!/usr/bin/python3
# -*- coding: utf-8 -*-

#
# Core functions used by multiple plugins, so they can be inherit from this class
#

class Plugin(object):

    filter_filename_suffixes = []
    filter_mimetype_prefixes = []

    # filter for mimetype prefixes or filename suffixes
    def filter(self, parameters=None, data=None):

        filtered = False
        
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        filename = None
        if 'filename' in parameters:
            filename = parameters['filename']

        mimetype = None
        if 'content_type_ss' in data:
            mimetype = data['content_type_ss']
        elif 'content_type_ss' in parameters:
            mimetype = parameters['content_type_ss']

        # if connector returns a list, use only first value
        # (which is the only entry or the main content type of file)
        if isinstance(mimetype, list):
            mimetype = mimetype[0]

        # is there a filename suffix match?
        match_filename_suffix = False
        if filename:
            for suffix in self.filter_filename_suffixes:
                if filename.lower().endswith(suffix.lower()):
                    if verbose:
                        print('Filename suffix matches plugin filter(s) {}'.format(self.filter_filename_suffixes))
    
                    match_filename_suffix = True
            
        # is there a mimetype prefix match?
        match_contenttype_prefix = False
        if mimetype:
            for prefix in self.filter_mimetype_prefixes:
                if mimetype.lower().startswith(prefix.lower()):
                    if verbose:
                        print('Contenttype matches plugin filter(s) {}'.format(self.filter_mimetype_prefixes))
    
                    match_contenttype_prefix = True

        # if filter(s) configured for file suffix or mimetype prefix, set filtered if matches
        if len(self.filter_mimetype_prefixes)>0 and len(self.filter_filename_suffixes) > 0:
            if not match_filename_suffix and not match_contenttype_prefix:
                if verbose:
                    print('Wether filename suffix nor content type matches plugin filter(s) for mimetypes {} or filename suffixes {}, so no further processing of this plugin'.format(self.filter_mimetype_prefixes, self.filter_filename_suffixes))
                filtered = True
        elif len(self.filter_mimetype_prefixes)>0:
            if not match_contenttype_prefix:
                if verbose:
                    print('Contenttype does not match plugin filter(s) {}, so no further processing of this plugin'.format(self.filter_mimetype_prefixes))
                filtered = True
        elif len(self.filter_filename_suffixes)>0:
            if not match_filename_suffix:
                if verbose:
                    print('Filename suffix does not match plugin filter(s) {}, so no further processing of this plugin'.format(self.filter_filename_suffixes))
                filtered = True
            
        return filtered


def get_text(data):
    text = ''

    #
    # exclude fields like technical metadata
    #

    exclude_fields_prefix = []

    listfile = open('/etc/opensemanticsearch/blacklist/textanalysis/blacklist-fieldname-prefix')
    for line in listfile:
        line = line.strip()
        if line and not line.startswith("#"):
            exclude_fields_prefix.append(line)
    listfile.close()

    # suffixes of non-text fields like nubers
    exclude_fields_suffix = []

    listfile = open('/etc/opensemanticsearch/blacklist/textanalysis/blacklist-fieldname-suffix')
    for line in listfile:
        line = line.strip()
        if line and not line.startswith("#"):
            exclude_fields_suffix.append(line)
    listfile.close()

    # full fieldnames
    exclude_fields = []
    listfile = open('/etc/opensemanticsearch/blacklist/textanalysis/blacklist-fieldname')
    for line in listfile:
        line = line.strip()
        if line and not line.startswith("#"):
            exclude_fields.append(line)
    listfile.close()


    for field in data:

        is_blacklisted = False

        for blacklisted_prefix in exclude_fields_prefix:
            if field.startswith(blacklisted_prefix):
                is_blacklisted = True
        
        for blacklisted_suffix in exclude_fields_suffix:
            if field.endswith(blacklisted_suffix):
                is_blacklisted = True
        
        if field in exclude_fields:
            is_blacklisted = True

        if not is_blacklisted:
    
            values = data[field]
    
            if not isinstance(values, list):
                values = [values]
    
            for value in values:
                if value:
                    text = "{}{}\n".format(text, value)
        
    return text


# append values (i.e. from an enhancer) to data structure
def append(data, facet, values):

    # if facet there yet, append/extend the values, else set values to facet
    if facet in data:

        # if new value(s) single value instead of list convert to list
        if not isinstance(values, list):
            values = [values]

        # if facet in data single value instead of list convert to list
        if not isinstance(data[facet], list):
            data[facet] = [data[facet]]

        # add new values to this list
        data[facet].extend(values)

        # dedupe data in facet
        data[facet] = list(set(data[facet]))

        # if only one value, it has not to be a list
        if len(data[facet]) == 1:
            data[facet] = data[facet][0]

    else:
        data[facet] = values


#
# Get preferred label(s) from field in format pref label and uri
#
def get_preflabels(values):

    uri2preflabel_map = {}

    if values:

        if not isinstance(values, list):
            values = [values]

        for value in values:
            pos_uri = value.rfind(' <')
            uri = value[pos_uri+2:-1]
            preflabel = value[0:pos_uri]
            uri2preflabel_map[uri] = preflabel

    return uri2preflabel_map


def get_all_matchtexts(values):
    
    results = {}

    if not isinstance(values, list):
        values = [values]

    for value in values:

        #get only matchtext (without ID/URI of matching entity)
        value = value.split("\t")
        matchid = value[0]
        matchtext = value[1]
        
        if not matchid in results:
            results[matchid] = []
            
        if not matchtext in results[matchid]:
            results[matchid].append(matchtext)

    return results