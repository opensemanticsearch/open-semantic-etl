import sys
import subprocess
from pathlib import Path
import hashlib

import etl_plugin_core


# generate single page PDF for each page of the full PDF for preview so client has not to load full pdf for previewing a page

class enhance_pdf_page_preview(etl_plugin_core.Plugin):
    
    # process plugin, if one of the filters matches
    filter_filename_suffixes = ['.pdf']
    filter_mimetype_prefixes = ['application/pdf']


    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        if verbose:
            print('Mimetype or filename suffix is PDF, extracting single pages for preview')

        if 'id' in data:
            docid = data['id']
        else:
            docid = parameters['id']

        filename = parameters['filename']

        thumbnail_dir = '/var/opensemanticsearch/media/thumbnails'

        # generate thumbnail directory
        md5hash = hashlib.md5(docid.encode('utf-8')).hexdigest()

        if not thumbnail_dir.endswith('/'):
            thumbnail_dir += '/'

        thumbnail_subdir = md5hash

        Path(thumbnail_dir + thumbnail_subdir).mkdir(parents=True, exist_ok=True)

        if verbose:
            print("Generating single page PDF for previews from {} for {} to {}".format(
                filename, docid, thumbnail_dir + thumbnail_subdir))

        # call pdftk burst
        try:
            result = subprocess.check_call(
                ['pdftk', filename, 'burst', 'output', thumbnail_dir + thumbnail_subdir + '/%d.pdf'])
            data['etl_thumbnails_s'] = thumbnail_subdir
        except BaseException as e:
            sys.stderr.write(
                "Exception while genarating single page PDFs by pdftk burst\n")

        return parameters, data
    