#!/usr/bin/python
# -*- coding: utf-8 -*-

import os.path
import datetime

#
# Add file modification time
#


class enhance_file_mtime(object):
    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        filename = parameters['filename']

        # get modification time from file
        file_mtime = os.path.getmtime(filename)

        # convert mtime to Lucene format
        file_mtime_masked = datetime.datetime.fromtimestamp(
            file_mtime).strftime("%Y-%m-%dT%H:%M:%SZ")

        if verbose:
            print("File modification time: {}".format(file_mtime_masked))

        data['file_modified_dt'] = file_mtime_masked

        return parameters, data
