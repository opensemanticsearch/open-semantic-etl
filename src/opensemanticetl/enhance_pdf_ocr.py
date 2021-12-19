import os.path
import sys
import subprocess
import hashlib
import tempfile
import json

import etl_plugin_core
from tesseract_cache import tesseract_cache


# Extract text from all extracted images from pdf
# if splitpages is off, return one txt instead of page based list of texts

def pdfimages2text(filename, lang='eng', verbose=False,
                   pdf_ocr=True,
                   cache=None):
    ocr_txt = {}
    if cache is not None:
        try:
            return load_cache(filename, cache, lang, pdf_ocr)
        except (FileNotFoundError, KeyError):
            if verbose:
                print('Not in PDF OCR cache, starting OCR for {}'.format(filename))

    ocr_temp_dirname = tempfile.mkdtemp(prefix="opensemanticetl_pdf_ocr_")

    # Extract all images of the pdf to tempdir with commandline tool
    # "pdfimages" from poppler pdf toolbox
    # -j = export as JPEG
    # -p = write page name in image filename
    result = subprocess.call(
        ['pdfimages', '-p', '-j', filename,
         ocr_temp_dirname + os.path.sep + 'image'])

    if result != 0:
        sys.stderr.write(
            "Error: Extracting images from PDF failed for {} {}"
            .format(filename, result))
        return {}, {}

    images = os.listdir(ocr_temp_dirname)
    images.sort()

    for image in images:

        imagefilename = ocr_temp_dirname + os.path.sep + image

        if pdf_ocr:

            try:
                result = tesseract_cache.get_ocr_text(filename=imagefilename, lang=lang, cache_dir=cache, verbose=verbose)

                if result:
                    # extract page number from extracted image
                    # filename (image-pagenumber-imagenumber.jpg)
                    pagenumber = int(image.split('-')[1])

                    append_page(ocr_txt, pagenumber, result)
            except BaseException as e:
                sys.stderr.write("Exception while OCR of PDF: {} - "
                                 "maybe corrupt image: {} - exception: {}\n"
                                 .format(filename, imagefilename, e))

        os.remove(imagefilename)

    os.rmdir(ocr_temp_dirname)
    return ocr_txt


def load_cache(filename, cache, lang='eng',
               pdf_ocr=True):
    pdffile = open(filename, 'rb')
    md5hash = hashlib.md5(pdffile.read()).hexdigest()
    pdffile.close()
    ocr_cache_filename = cache + os.path.sep + \
        "{}-{}.json".format(lang, md5hash)
    with open(ocr_cache_filename) as f:
        dct = json.load(f)
        ocr_txt = None
        if pdf_ocr:
            ocr_txt = dict(enumerate(dct["ocr_txt"], 1))
        return ocr_txt


def append_page(dct, n, page):
    if n in dct:
        dct[n] += '\n' + page
    else:
        dct[n] = page

#
# Process plugin
#
# check if content type PDF, if so start enrich pdf process for OCR
#

class enhance_pdf_ocr(etl_plugin_core.Plugin):

    # process plugin, if one of the filters matches
    filter_filename_suffixes = ['.pdf']
    filter_mimetype_prefixes = ['application/pdf']

    # how to find uris which are not enriched yet?
    # (if not enhanced on indexing but later)

    # this plugin needs to read the field id as a parameters
    # to enrich unenriched docs
    fields = ['id', 'content_type']

    # query to find documents, that were not enriched by this plugin yet
    # (since we marked documents which were OCRd with ocr_b = true
    query = ("(content_type:application/pdf*) "
             "AND NOT (etl_enhance_pdf_ocr_b:true)")

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        verbose = parameters.get('verbose', False)

        # no further processing, if plugin filters like for content type do not match
        if self.filter(parameters, data):
            return parameters, data
    
        filename = parameters['filename']

        # is OCR of embedded images by Tika enabled or disabled by config?
        ocr_pdf_tika = parameters.get('ocr_pdf_tika', True)

        # was there a Tika exception?
        tika_exception = parameters.get('etl_tika_exception', False)
        if 'etl_error_plugins_ss' in data:
            if 'enhance_extract_text_tika_server' in data['etl_error_plugins_ss']:
                tika_exception = True


        # OCR is done by Apache Tika plugin
        # If standard OCR by Tika is disabled or Tika Exception, do it here
        pdf_ocr = False

        # Do not run if no images (detected by Tika plugin)
        nothing_for_ocr = parameters.get('etl_nothing_for_ocr', False)

        if nothing_for_ocr:

            if verbose:
                print('Not running OCR for PDF, since no image(s) detected by Apache Tika')
            
            pdf_ocr = False

        elif tika_exception or ocr_pdf_tika == False:
            pdf_ocr = True

        if pdf_ocr:
    
            if verbose:
                print('Mimetype is PDF or file ending is .pdf, running OCR of embedded images')

                if not ocr_pdf_tika:
                    print ('OCR of embedded images in PDF by Apache Tika is disabled, so doing OCR for PDF by plugin enhance_pdf_ocr')
                elif tika_exception:
                    print ('Because of Apache Tika exception, adding / trying fallback OCR for PDF by plugin enhance_pdf_ocr')

            lang = parameters.get('ocr_lang', 'eng')
    
            ocr_txt = {}

            try:
                ocr_txt = pdfimages2text(
                    filename=filename, lang=lang, verbose=verbose,
                    pdf_ocr=pdf_ocr,
                    cache=parameters.get("ocr_cache"))
            except BaseException as e:
                sys.stderr.write(
                    "Exception while OCR the PDF {} - {}\n".format(filename, e))
        
            parameters['enhance_pdf_ocr'] = ocr_txt

            # create text field ocr_t with all OCR results of all pages
            pages_content = [value for (key, value) in sorted(ocr_txt.items())]
            data['ocr_t'] = "\n".join(pages_content)

        return parameters, data
