#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import sys
import time

import urllib.request
import urllib.parse

# Export data to Solr


class export_solr(object):

    def __init__(self, config=None):
        if config is None:
            config = {}

        self.config = config

        if os.getenv('OPEN_SEMANTIC_ETL_SOLR'):
            self.config['solr'] = os.getenv('OPEN_SEMANTIC_ETL_SOLR')

        if not 'solr' in self.config:
            self.config['solr'] = 'http://localhost:8983/solr/'

        if not 'index' in self.config:
            self.config['index'] = 'opensemanticsearch'

        self.solr = self.config['solr']
        self.core = self.config['index']

        if not 'verbose' in self.config:
            self.config['verbose'] = False

        self.verbose = self.config['verbose']

    #
    # Write data to Solr
    #

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        # if not there, set config defaults
        if 'verbose' in parameters:
            self.verbose = parameters['verbose']

        if self.verbose:            
            print ('Starting Exporter: Solr')

        if 'solr' in parameters:
            self.solr = parameters['solr']
            if not self.solr.endswith('/'):
                self.solr += '/'

        if 'index' in parameters:
            self.core = parameters['index']

        add = parameters.get('add', False)

        fields_set = parameters.get('fields_set', [])

        commit = parameters.get('commit', None)

        if not 'id' in data:
            data['id'] = parameters['id']

        # post data to Solr
        do_export = True

        # but do not post, if only id (which will contain no add or set commands for fields and will be seen as overwrite for whole document)
        if len(data) < 2:
            if self.verbose:
                print('Not exported to Solr because no data or only the ID.')
            do_export = False

        # and do not post, if yet posted before (exporter not exporter, but in plugin queue, f.e. in multi stage processing before adding task to queue)
        if 'etl_export_solr_b' in data:
            if self.verbose:
                print('Not exported to Solr because no data or yet exported in this ETL run, because exporter was runned as plugin.')
                do_export = False

        if do_export:
            self.update(data=data, add=add, fields_set=fields_set, commit=commit)


        return parameters, data

    # update document in index, set fields in data to updated or new values or add new/additional values
    # if no document yet, it will be added
    def update(self, data, add=False, fields_set=(), commit=None):

        update_fields = {}

        for fieldname in data:
            if fieldname == 'id':
                update_fields['id'] = data['id']
            else:
                update_fields[fieldname] = {}

                if add and not fieldname in fields_set:
                    # add value to existent values of the field
                    update_fields[fieldname]['add-distinct'] = data[fieldname]
                else:
                    # if document there with values for this fields, the existing values will be overwritten with new values
                    update_fields[fieldname]['set'] = data[fieldname]

        self.post(data=update_fields, commit=commit)

    def post(self, data=None, docid=None, commit=None):
        if data is None:
            data = {}

        solr_uri = self.solr + self.core + '/update'

        if docid:
            data['id'] = docid

        datajson = '[' + json.dumps(data) + ']'

        params = {}

        if commit:
            params['commit'] = 'true'

        if self.verbose:
            print("Sending update request to {}".format(solr_uri))
            print(datajson)


        retries = 0
        retrytime = 1
        # wait time until next retry will be doubled until reaching maximum of 120 seconds (2 minutes) until next retry
        retrytime_max = 120
        no_connection = True

        while no_connection:
            try:
                if retries > 0:
                    print('Will retry to connect to Solr in {} second(s).'.format(retrytime))
                    time.sleep(retrytime)
                    retrytime = retrytime * 2
                    if retrytime > retrytime_max:
                        retrytime = retrytime_max

                r = requests.post(solr_uri, data=datajson, params=params, headers={'Content-Type': 'application/json'})

                # if bad status code, raise exception
                r.raise_for_status()

                if retries > 0:
                    print('Successfull retried to connect Solr.')

                no_connection = False

            except KeyboardInterrupt:
                raise KeyboardInterrupt

            except requests.exceptions.ConnectionError as e:

                    retries += 1

                    sys.stderr.write("Connection to Solr failed (will retry in {} seconds). Exception: {}\n".format(retrytime, e))

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 503:

                    retries += 1

                    sys.stderr.write("Solr temporary unavailable (HTTP status code 503). Will retry in {} seconds). Exception: {}\n".format(retrytime, e))

                else:
                    no_connection = False

                    sys.stderr.write('Error while posting data to Solr: {}'.format(e))

                    raise(e)

            except BaseException as e:
                no_connection = False

                sys.stderr.write('Error while posting data to Solr: {}'.format(e))

                raise(e)


    # tag a document by adding new value to field
    def tag(self, docid=None, field=None, value=None, data=None):
        if data is None:
            data = {}

        data_merged = data.copy()

        if docid:
            data_merged['id'] = docid

        if field:
            if field in data_merged:
                # if not list, convert to list
                if not isinstance(data_merged[field], list):
                    data_merged[field] = [data_merged[field]]
                # add value to field
                data_merged[field].append(value)
            else:
                data_merged[field] = value

        result = self.update(data=data_merged, add=True)

        return result

    # search for documents with query and without tag and update them with the tag
    def update_by_query(self, query, field=None, value=None, data=None, queryparameters=None):
        if data is None:
            data = {}

        import pysolr

        count = 0

        solr = pysolr.Solr(self.solr + self.core)

        #
        # extend query: do not return documents, that are tagged
        #

        query_marked_before = ''

        if field:
            query_marked_before = field + ':"' + solr_mask(value) + '"'

        # else extract field and value from data to build query of yet tagged docs to exclude

        for fieldname in data:

            if isinstance(data[fieldname], list):

                for value in data[fieldname]:

                    if query_marked_before:
                        query_marked_before += " AND "

                    query_marked_before += fieldname + \
                        ':"' + solr_mask(value) + '"'
            else:

                value = data[fieldname]
                if query_marked_before:
                    query_marked_before += " AND "

                query_marked_before += fieldname + \
                    ':"' + solr_mask(value) + '"'

        solrparameters = {
            'fl': 'id',
            'defType': 'edismax',
            'rows': 10000000,
        }

        # add custom Solr parameters (if the same parameter, overwriting the obove defaults)
        if queryparameters:
            solrparameters.update(queryparameters)

        if query_marked_before:
            # don't extend query but use filterquery for more performance (cache) on aliases
            solrparameters["fq"] = 'NOT (' + query_marked_before + ')'

        if self.verbose:
            print("Solr query:")
            print(query)
            print("Solr parameters:")
            print(solrparameters)

        results = solr.search(query, **solrparameters)

        for result in results:
            docid = result['id']

            if self.verbose:
                print("Tagging {}".format(docid))

            self.tag(docid=docid, field=field, value=value, data=data)

            count += 1

        return count

    def get_data(self, docid, fields):

        uri = self.solr + self.core + '/get?id=' + \
            urllib.parse.quote(docid) + '&fl=' + ','.join(fields)

        request = urllib.request.urlopen(uri)
        encoding = request.info().get_content_charset('utf-8')
        data = request.read()
        request.close()

        solr_doc = json.loads(data.decode(encoding))

        data = None
        if 'doc' in solr_doc:
            data = solr_doc['doc']

        return data

    def commit(self):

        uri = self.solr + self.core + '/update?commit=true'
        if self.verbose:
            print("Committing to {}".format(uri))
        request = urllib.request.urlopen(uri)
        request.close()

    def get_lastmodified(self, docid):
        # convert mtime to solr format
        solr_doc_mtime = None

        solr_doc = self.get_data(docid=docid, fields=["file_modified_dt"])

        if solr_doc:
            if 'file_modified_dt' in solr_doc:
                solr_doc_mtime = solr_doc['file_modified_dt']

            # todo: for each plugin
#			solr_meta_mtime = False
#			if 'meta_modified_dt' in solr_doc['doc']:
#				solr_meta_mtime = solr_doc['doc']['meta_modified_dt']

        return solr_doc_mtime

    def delete(self, parameters, docid=None, query=None,):
        import pysolr

        if 'solr' in parameters:
            self.solr = parameters['solr']
            if not self.solr.endswith('/'):
                self.solr += '/'

        if 'index' in parameters:
            self.core = parameters['index']

        solr = pysolr.Solr(self.solr + self.core)

        if docid:
            result = solr.delete(id=docid)

        if query:
            result = solr.delete(q=query)

        return result

    #
    # append synonyms by Solr REST API for managed resources
    #

    def append_synonyms(self, resourceid, synonyms):

        url = self.solr + self.core + '/schema/analysis/synonyms/' + resourceid
        headers = {'content-type': 'application/json'}

        r = requests.post(url=url, data=json.dumps(synonyms), headers=headers)


def solr_mask(string_to_mask, solr_specialchars='\+-&|!(){}[]^"~*?:/'):

    masked = string_to_mask
    # mask every special char with leading \
    for char in solr_specialchars:
        masked = masked.replace(char, "\\" + char)

    return masked
