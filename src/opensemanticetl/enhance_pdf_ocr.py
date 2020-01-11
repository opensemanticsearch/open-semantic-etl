import os.path
import sys
import subprocess
import hashlib
import tempfile
import json
import enhance_ocr
import enhance_ocr_descew

# Extract text from all extracted images from pdf
# if splitpages is off, return one txt instead of page based list of texts


def pdfimages2text(filename, lang='eng', verbose=False,
                   pdf_ocr=True, pdf_ocr_descew=False,
                   cache=None):
    ocr_txt = {}
    ocr_descew_txt = {}
    if cache is not None:
        try:
            return load_cache(filename, cache, lang, pdf_ocr, pdf_ocr_descew)
        except (FileNotFoundError, KeyError):
            if verbose:
                print('Not in OCR cache, starting OCR for {}'.format(filename))

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
                result = enhance_ocr.image2text(
                    filename=imagefilename, lang=lang, verbose=verbose)

                if result:
                    # extract page number from extracted image
                    # filename (image-pagenumber-imagenumber.jpg)
                    pagenumber = int(image.split('-')[1])

                    append_page(ocr_txt, pagenumber, result)
            except BaseException as e:
                sys.stderr.write("Exception while OCR of PDF: {} - "
                                 "maybe corrupt image: {} - exception: {}\n"
                                 .format(filename, imagefilename, e))

        if pdf_ocr_descew:

            try:
                result = enhance_ocr_descew.optimized_image2text(
                    imagefilename, lang, verbose=verbose)

                if result:
                    # extract page number from extracted image
                    # filename (image-pagenumber-imagenumber.jpg)
                    pagenumber = int(image.split('-')[1])
                    append_page(enhance_ocr_descew, pagenumber, result)

            except BaseException as e:

                sys.stderr.write(
                    "Exception while optimized ocr pdf: {} - "
                    "maybe corrupt image: {} - exception: {}\n"
                    .format(filename, imagefilename, e))

        os.remove(imagefilename)

    os.rmdir(ocr_temp_dirname)
    return ocr_txt, ocr_descew_txt


def load_cache(filename, cache, lang='eng',
               pdf_ocr=True, pdf_ocr_descew=False):
    md5hash = hashlib.md5(open(filename, 'rb').read()).hexdigest()
    ocr_cache_filename = cache + os.path.sep + \
        "{}-{}.json".format(lang, md5hash)
    with open(ocr_cache_filename) as f:
        dct = json.load(f)
        ocr_txt = None
        ocr_descew_txt = None
        if pdf_ocr:
            ocr_txt = dict(enumerate(dct["ocr_txt"], 1))
        if pdf_ocr_descew:
            ocr_descew_txt = dict(enumerate(dct["ocr_descew_txt"], 1))
        return ocr_txt, ocr_descew_txt


def append_page(dct, n, page):
    if n in dct:
        dct[n] += '\n' + page
    else:
        dct[n] = page


def enrich_pdf(parameters=None, data=None):
    if parameters is None:
        parameters = {}
    if data is None:
        data = {}

    verbose = parameters.get('verbose', False)

    filename = parameters['filename']

    pdf_ocr_descew = ('enhance_ocr_descew' in parameters['plugins'])

    if 'ocr_lang' in parameters:
        lang = parameters['ocr_lang']
    else:
        lang = 'eng'

    ocr_txt = {}
    ocr_optimized_txt = {}

    try:
        ocr_txt, ocr_optimized_txt = pdfimages2text(
            filename=filename, lang=lang, verbose=verbose,
            pdf_ocr=True, pdf_ocr_descew=pdf_ocr_descew,
            cache=parameters.get("ocr_cache"))
    except BaseException as e:
        sys.stderr.write(
            "Exception while OCR the PDF {} - {}\n".format(filename, e))

    parameters['enhance_pdf_ocr'] = ocr_txt
    parameters['enhance_pdf_ocr_descew'] = ocr_optimized_txt

    # create text field ocr_t with all OCR results of all pages
    pages_content = [value for (key, value) in sorted(ocr_txt.items())]
    data['ocr_t'] = "\n".join(pages_content)


    if pdf_ocr_descew:
        pages_content = [value for (key, value) in sorted(
            ocr_optimized_txt.items())]
        data['ocr_descew_t'] = "\n".join(pages_content)

    # Mark document to enhanced with this plugin
    data['etl_enhance_pdf_ocr_b'] = True

    return parameters, data


#
# Process plugin
#
# check if content type PDF, if so start enrich pdf process for OCR
#

class enhance_pdf_ocr:

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

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        filename = parameters['filename']

        mimetype = ''
        if 'content_type_ss' in data:
            mimetype = data['content_type_ss']
        elif 'content_type_ss' in parameters:
            mimetype = parameters['content_type_ss']

        # if connector returns a list, use only first value
        # (which is the only entry of the list)
        if isinstance(mimetype, list):
            mimetype = mimetype[0]

        if "application/pdf" in mimetype.lower() \
           or filename.lower().endswith('.pdf'):
            if verbose:
                print('Mimetype is PDF ({}) or file ending is PDF, '
                      'starting OCR of embedded images'.format(
                          mimetype))

            parameters, data = enrich_pdf(parameters, data)

        return parameters, data
