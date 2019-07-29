#!/usr/bin/python
# -*- coding: utf-8 -*-

import magic


#
# Get MimeType (Which kind of file is this?)
#
class enhance_mimetype(object):

    def process(self, parameters={}, data={}):

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        filename = parameters['filename']

        mimetype = None

        m = magic.open(magic.MAGIC_MIME)
        m.load()
        mimetype = m.file(filename)
        m.close()

        if verbose:
            print("Detected MimeType: {}".format(mimetype))

        data['content_type_magic_s'] = mimetype

        return parameters, data
