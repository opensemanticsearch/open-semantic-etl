import os
import shutil
import sys
import tempfile
import subprocess

import etl_plugin_core
from tesseract_cache import tesseract_cache

#
# Optimize image with Scantailor before OCR
#
# Should be runned additionally to normal unoptimized OCR
# because sometimes Scantailor results incomplete with fewer text parts than original images


def optimized_image2text(filename, lang='eng', cache_dir=None, verbose=False):

    ocr_txt = False

    ocr_temp_dirname = tempfile.mkdtemp(prefix="opensemanticetl_ocr_descew_")

    if verbose:
        print("Optimizing image {}".format(filename))

    # start external OCR Program
    result = subprocess.call(['scantailor-cli', filename, ocr_temp_dirname])

    if result == 0:

        images = os.listdir(ocr_temp_dirname)
        images.sort()

        for image in images:

            try:
                result = False

                imagefilename = ocr_temp_dirname + os.path.sep + image

                # ignore the cache directory of scantailor, only files in directory
                if os.path.isfile(imagefilename):

                    result = tesseract_cache.get_ocr_text(filename=imagefilename, lang=lang, cache_dir=cache_dir, verbose=verbose)

                    os.remove(imagefilename)

                    if result:

                        if ocr_txt:
                            ocr_txt = ocr_txt + '\n' + result
                        else:
                            ocr_txt = result

            except KeyboardInterrupt:
                raise KeyboardInterrupt
            except BaseException as e:
                sys.stderr.write(
                    "Exception while OCR descewed image of: {} - Maybe descewed image {} corrupt? Exception: {}\n" .format(filename, imagefilename, e))

    else:
        sys.stderr.write(
            "Error: Descewing images for OCR failed for {} with return code {}".format(filename, result))

    shutil.rmtree(ocr_temp_dirname)

    return ocr_txt


#
# If image add ocr text from optimized image
#

class enhance_ocr_descew(etl_plugin_core.Plugin):

    # process plugin, if one of the filters matches
    filter_mimetype_prefixes = ['image']

    # how to find uris which are not enriched yet?
    # (if not enhanced on indexing but later)

    # this plugin needs to read the field id as a parameters to enrich unenriched docs
    fields = ['id', 'content_type_ss']

    # query to find documents, that were not enriched by this plugin yet
    # (since we marked documents which were OCRd with ocr_b = true
    query = "content_type_ss: image\/* AND NOT enhance_ocr_descew_b:true"

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        verbose = parameters.get('verbose', False)

        filename = parameters['filename']

        lang = parameters.get('ocr_lang', 'eng')

        if verbose:
            print("Mimetype seems image, starting optimized OCR")

        ocr_txt = optimized_image2text(filename=filename, lang=lang, cache_dir=parameters.get("ocr_cache"), verbose=verbose)

        if ocr_txt:
            data['ocr_descew_t'] = ocr_txt

        return parameters, data
