import hashlib
import tempfile
import os
import sys
import shutil
import time

from warcio.archiveiterator import ArchiveIterator

import etl_plugin_core
from etl_file import Connector_File


class enhance_warc(etl_plugin_core.Plugin):

    # process plugin, if one of the filters matches
    filter_filename_suffixes = ['.warc', '.warc.gz']
    filter_mimetype_prefixes = ['application/warc']

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        # no further processing, if plugin filters like for content type do not match
        if self.filter(parameters, data):
            return parameters, data

        warcfilename = parameters['filename']

        # create temp dir where to unwarc the archive
        if 'tmp' in parameters:
            system_temp_dirname = parameters['tmp']
            if not os.path.exists(system_temp_dirname):
                os.mkdir(system_temp_dirname)
        else:
            system_temp_dirname = tempfile.gettempdir()

        # we build temp dirname ourselfes instead of using system_temp_dirname so we can use configurable / external tempdirs
        h = hashlib.md5(parameters['id'].encode('UTF-8'))
        temp_dirname = system_temp_dirname + os.path.sep + \
            "opensemanticetl_enhancer_warc_" + h.hexdigest()

        if os.path.exists(temp_dirname) == False:
            os.mkdir(temp_dirname)

        # prepare document processing
        connector = Connector_File()
        connector.verbose = verbose
        connector.config = parameters.copy()

        # only set container if not yet set by a zip before (if this zip is inside another zip)
        if not 'container' in connector.config:
            connector.config['container'] = warcfilename

        i = 0

        with open(warcfilename, 'rb') as stream:
            for record in ArchiveIterator(stream):
                i += 1

                if record.rec_type == 'response':

                    print(record.rec_headers)

                    # write WARC record content to tempfile
                    tempfilename = temp_dirname + \
                        os.path.sep + 'warcrecord' + str(i)
                    tmpfile = open(tempfilename, 'wb')
                    tmpfile.write(record.content_stream().read())
                    tmpfile.close()

                    # set last modification time of the file to WARC-Date
                    try:
                        last_modified = time.mktime(time.strptime(
                            record.rec_headers.get_header('WARC-Date'), '%Y-%m-%dT%H:%M:%SZ'))
                        os.utime(tempfilename, (last_modified, last_modified))
                    except BaseException as e:
                        sys.stderr.write("Exception while reading filedate to warc content {} from {} : {}\n".format(
                            tempfilename, connector.config['container'], e))

                    # set id (URL and WARC Record ID)
                    connector.config['id'] = record.rec_headers.get_header(
                        'WARC-Target-URI') + '/' + record.rec_headers.get_header('WARC-Record-ID')

                    # index the extracted file
                    try:

                        connector.index_file(filename=tempfilename)

                    except KeyboardInterrupt:
                        raise KeyboardInterrupt

                    except BaseException as e:
                        sys.stderr.write("Exception while indexing warc content {} from {} : {}\n".format(
                            tempfilename, connector.config['container'], e))

                    os.remove(tempfilename)

        shutil.rmtree(temp_dirname)

        return parameters, data
