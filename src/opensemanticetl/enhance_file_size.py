#!/usr/bin/python
# -*- coding: utf-8 -*-

import os.path

#
# add file size
#


class enhance_file_size(object):
    def process(self, parameters={}, data={}):

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        filename = parameters['filename']

        # get filesize
        file_size = os.path.getsize(filename)

        if verbose:
            print("File size: {}".format(file_size))

        data['file_size_i'] = str(file_size)

        return parameters, data
