#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import datetime
import sys
import importlib

#
# do not index (set parameters["break"] = True), if yet former ETL with all configured plugins / yet indexed
#


class filter_file_not_modified(object):

    def __init__(self):

        self.verbose = False
        self.quiet = False

        # if a critical plugin failed in former ETL / indexed document, reindex file to retry
        self.force_reindex_if_former_etl_plugin_errors = [
            'enhance_extract_text_tika_server']

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        if 'verbose' in parameters:
            if parameters['verbose']:
                self.verbose = True

        if 'quiet' in parameters:
            self.quiet = parameters['quiet']

        filename = parameters['filename']

        force = False

        if 'force' in parameters:
            force = parameters['force']

        # check if file size and file date are the same in DB
        # if exist delete protocoll prefix file://
        if filename.startswith("file://"):
            filename = filename.replace("file://", '', 1)

        # if relative path change to absolute path
        filename = os.path.abspath(filename)

        # get modification time from file
        file_mtime = os.path.getmtime(filename)

        # get id
        docid = parameters['id']

        export = False
        indexed_doc_mtime = None
        plugins_failed = []
        critical_plugins_failed = []
        plugins_runned = []
        plugins_not_runned = []
        additional_plugins_later_not_runned = []
        do_not_reindex_because_plugin_yet_not_processed = []

        # use abstracted function from exporter module to get last modification time of file in index
        if 'export' in parameters:
            export = parameters['export']
            module = importlib.import_module(export)
            objectreference = getattr(module, export)
            exporter = objectreference(parameters)

            # get modtime and ETL errors from document saved in index
            metadatafields = ['file_modified_dt', 'etl_error_plugins_ss']

            # get plugin status fields
            for configured_plugin in parameters['plugins']:
                if not configured_plugin == 'export_queue_files' and not configured_plugin == parameters['export']:
                    metadatafields.append('etl_' + configured_plugin + '_b')
            if 'additional_plugins_later' in parameters:
                for configured_plugin in parameters['additional_plugins_later']:
                    metadatafields.append('etl_' + configured_plugin + '_b')

            # get config option status field for OCR
            if 'ocr' in parameters:
                if parameters['ocr']:
                    metadatafields.append(
                        'etl_enhance_extract_text_tika_server_ocr_enabled_b')
            if 'additional_plugins_later_config' in parameters:
                if 'ocr' in parameters['additional_plugins_later_config']:
                    if parameters['additional_plugins_later_config']['ocr']:
                        metadatafields.append(
                            'etl_enhance_extract_text_tika_server_ocr_enabled_b')

            if 'do_not_reindex_because_plugin_yet_not_processed' in parameters:
                do_not_reindex_because_plugin_yet_not_processed=parameters['do_not_reindex_because_plugin_yet_not_processed']

            # read yet indexed metadata, if there
            indexed_metadata = exporter.get_data(
                docid=docid, fields=metadatafields)

            if indexed_metadata:
                if 'file_modified_dt' in indexed_metadata:
                    indexed_doc_mtime = indexed_metadata['file_modified_dt']
                if 'etl_error_plugins_ss' in indexed_metadata:
                    plugins_failed = indexed_metadata['etl_error_plugins_ss']

        # mask file_mtime for comparison in same format than in Lucene index
        file_mtime_masked = datetime.datetime.fromtimestamp(
            file_mtime).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Is it a new file (not indexed, so the initial None different to filemtime)
        # or modified (also doc_mtime <> file_mtime of file)?

        if indexed_doc_mtime == file_mtime_masked:

            # Doc was found in index and field moddate of solr doc same as files mtime
            # so file was indexed before and is unchanged

            # all now configured plugins processed in former ETL/their analysis is in index?
            for configured_plugin in parameters['plugins']:
                if not configured_plugin == 'export_queue_files' and not configured_plugin == parameters['export']:
                    plugin_runned = indexed_metadata.get('etl_' + configured_plugin + '_b', False)
                    if plugin_runned:
                        plugins_runned.append(configured_plugin)
                    else:
                        if not configured_plugin in do_not_reindex_because_plugin_yet_not_processed:
                            plugins_not_runned.append(configured_plugin)

            if 'additional_plugins_later' in parameters:
                for configured_plugin in parameters['additional_plugins_later']:
                    plugin_runned = indexed_metadata.get('etl_' + configured_plugin + '_b', False)
                    if plugin_runned:
                        plugins_runned.append(configured_plugin)
                    else:
                        if not configured_plugin in do_not_reindex_because_plugin_yet_not_processed:
                            additional_plugins_later_not_runned.append(configured_plugin)

            # Tika OCR was enabled in former ETL/their analysis is in index?
            if 'ocr' in parameters:
                if parameters['ocr']:
                    plugin_runned = indexed_metadata.get('etl_enhance_extract_text_tika_server_ocr_enabled_b', False)
                    if plugin_runned:
                        plugins_runned.append('etl_enhance_extract_text_tika_server_ocr_enabled_b')
                    else:
                        if not configured_plugin in do_not_reindex_because_plugin_yet_not_processed:
                            plugins_not_runned.append('etl_enhance_extract_text_tika_server_ocr_enabled_b')

            if 'additional_plugins_later_config' in parameters:
                if 'ocr' in parameters['additional_plugins_later_config']:
                    if parameters['additional_plugins_later_config']['ocr']:
                        plugin_runned = indexed_metadata.get('etl_enhance_extract_text_tika_server_ocr_enabled_b', False)
                        if plugin_runned:
                            plugins_runned.append('etl_enhance_extract_text_tika_server_ocr_enabled_b')
                        else:
                            if not configured_plugin in do_not_reindex_because_plugin_yet_not_processed:
                                additional_plugins_later_not_runned.append('etl_enhance_extract_text_tika_server_ocr_enabled_b')

            for critical_plugin in self.force_reindex_if_former_etl_plugin_errors:
                if critical_plugin in plugins_failed:
                    critical_plugins_failed.append(critical_plugin)

            if len(plugins_not_runned) > 0 or len(additional_plugins_later_not_runned) > 0:

                doindex = True

                # print status
                if self.verbose or self.quiet == False:
                    try:
                        print('Repeating indexing of unchanged file because (additional configured) plugin(s) or options {} not runned yet: {}'.format(
                            plugins_not_runned + additional_plugins_later_not_runned, filename))
                    except:
                        sys.stderr.write(
                            "Repeating indexing of unchanged file because former fail of critical plugin, but exception while printing message (problem with encoding of filename or console? Is console set to old ASCII standard instead of UTF-8?)")

                if len(plugins_not_runned) == 0:
                    parameters['only_additional_plugins_later'] = True

            # a critical plugin failed in former ETL
            elif len(critical_plugins_failed) > 0:

                doindex = True

                # print status
                if self.verbose or self.quiet == False:
                    try:
                        print('Repeating indexing of unchanged file because critical plugin(s) {} failed in former run: {}'.format(
                            critical_plugins_failed, filename))
                    except:
                        sys.stderr.write(
                            "Repeating indexing of unchanged file because critical plugin(s) failed in former run, but exception while printing message (problem with encoding of filename or console? Is console set to old ASCII standard instead of UTF-8?)")

            # If force option, do further processing even if unchanged
            elif force:

                doindex = True

                # print status
                if self.verbose or self.quiet == False:
                    try:
                        print(
                            'Forced indexing of unchanged file: {}'.format(filename))
                    except:
                        sys.stderr.write(
                            "Forced indexing of unchanged file but exception while printing message (problem with encoding of filename or console? Is console set to old ASCII standard instead of UTF-8?)")

            else:

                doindex = False

                # print status
                if self.verbose:
                    try:
                        print("Not indexing unchanged file: {}".format(filename))
                    except:
                        sys.stderr.write(
                            "Not indexing unchanged file but exception while printing message (problem with encoding of filename or console?)")

        else:  # doc not found in index or other/old modification time in index

            doindex = True

            # print status, if new document
            if self.verbose or self.quiet == False:

                if indexed_doc_mtime == None:
                    try:
                        print("Indexing new file: {}".format(filename))
                    except:
                        sys.stderr.write(
                            "Indexing new file but exception while printing message (problem with encoding of filename or console?)")
                else:
                    try:
                        print('Indexing modified file: {}'.format(filename))
                    except:
                        sys.stderr.write(
                            "Indexing modified file. Exception while printing filename (problem with encoding of filename or console?)\n")

        # if not modifed and no critical ETL errors, stop ETL process, because all done on last run
        if not doindex:
            parameters['break'] = True
        else:
            # reset plugin status of plugins of next stage
            # so reprocessing of updated data works by tasks in later stages,
            # which else would have plugin status processed
            # from first/last processing of old version of content

            commit = False
            if len(plugins_runned) > 0:
                
                for runned_plugin in plugins_runned:
                    if not runned_plugin in [parameters['export'], 'enhance_mapping_id', 'filter_blacklist', 'filter_file_not_modified']:
                        data['etl_' + runned_plugin + '_b'] = False
                        commit = True

            # immediatelly commit (else Solr autocommit after some time) of etl status reset(s) in exporter before adding new ETL tasks which need the status for plugin filter_file_not_modified
            if commit:
                parameters['commit'] = True

        return parameters, data
