import os
import sys
import subprocess
import tempfile
import hashlib

import etl_plugin_core
from etl import ETL

#
# by split to pages (so we have links to pages instead of documents) and get text from OCR from previous running plugin enhance_pdf_ocr and run plugins for splitting results into paragraphs and sentences
#


class enhance_pdf_page(etl_plugin_core.Plugin):

    # process plugin, if one of the filters matches
    filter_filename_suffixes = ['.pdf']
    filter_mimetype_prefixes = ['application/pdf']

    # how to find uris which are not enriched yet?
    # (if not enhanced on indexing but later)

    # this plugin needs to read the field id as a parameters to enrich unenriched docs
    fields = ['id', 'content_type']

    # query to find documents, that were not enriched by this plugin yet
    # (since we marked documents which were OCRd with ocr_b = true
    query = "content_type: application\/pdf* AND NOT enhance_pdf_page_b:true"

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

        if verbose:
            print('Mimetype or filename suffix is PDF, extracting single pages for segmentation')

        if 'id' in data:
            docid = data['id']
        else:
            docid = parameters['id']

        filename = parameters['filename']

        # defaults, if pdfinfo will not detect them
        pages = 1
        title = 'No title'
        author = None

        # get pagecount with pdfinfo command line tool
        pdfinfo = subprocess.check_output(
            ['pdfinfo', '-enc', 'UTF-8', filename])

        # decode
        pdfinfo = pdfinfo.decode(encoding='UTF-8')

        # get the count of pages from pdfinfo result
        # its a text with a line per parameter
        for line in pdfinfo.splitlines():
            line = line.strip()
            # we want only the line with the pagecount
            if line.startswith('Pages:'):
                pages = int(line.split()[1])

            if line.startswith('Title:'):
                title = line.replace("Title:", '', 1)
                title = title.strip()

            if line.startswith('Author:'):
                author = line.replace("Author:", '', 1)
                author = author.strip()

        etl = ETL()

        # export and index each page
        for pagenumber in range(1, pages + 1):

            if verbose:
                print("Extracting PDF page {} of {}".format(pagenumber, pages))
            # generate temporary filename
            md5hash = hashlib.md5(filename.encode('utf-8')).hexdigest()
            temp_filename = tempfile.gettempdir() + os.path.sep + \
                "opensemanticetl_pdftotext_" + md5hash + "_" + str(pagenumber)

            # call pdftotext to write the text of page into tempfile
            try:
                result = subprocess.check_call(['pdftotext', '-enc', 'UTF-8', '-f', str(
                    pagenumber), '-l', str(pagenumber), filename, temp_filename])
            except BaseException as e:
                sys.stderr.write(
                    "Exception extracting text from PDF page {}: {}\n".format(pagenumber, e))

            # read text from tempfile
            f = open(temp_filename, "r", encoding="utf-8")
            text = f.read()
            os.remove(temp_filename)

            partdocid = docid + '#page=' + str(pagenumber)

            partparameters = parameters.copy()
            partparameters['plugins'] = ['enhance_path', 'enhance_detect_language_tika_server',
                                         'enhance_entity_linking', 'enhance_multilingual']

            if 'enhance_ner_spacy' in parameters['plugins']:
                partparameters['plugins'].append('enhance_ner_spacy')
            if 'enhance_ner_stanford' in parameters['plugins']:
                partparameters['plugins'].append('enhance_ner_stanford')

            pagedata = {}
            pagedata['id'] = partdocid

            pagedata['page_i'] = pagenumber
            pagedata['pages_i'] = pages
            pagedata['container_s'] = docid
            pagedata['title_txt'] = title

            if author:
                pagedata['author_ss'] = author

            pagedata['content_type_group_ss'] = "Page"
            pagedata['content_type_ss'] = "PDF page"
            pagedata['content_txt'] = text

            if verbose:
                print("Indexing extracted page {}".format(pagenumber))

            # index page
            try:
                partparameters, pagedata = etl.process(
                    partparameters, pagedata)

            except BaseException as e:
                sys.stderr.write(
                    "Exception adding PDF page {} : {}".format(pagenumber, e))

        data['pages_i'] = pages

        return parameters, data
