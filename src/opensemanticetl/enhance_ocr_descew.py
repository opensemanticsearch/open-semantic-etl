import os
import shutil
import sys
import tempfile
import subprocess
import enhance_ocr

#
# Optimize image with Scantailor before OCR
#
# Should be runned additionally to normal unoptimized OCR
# because sometimes Scantailor results incomplete with fewer text parts than original images


def optimized_image2text(filename, lang='eng', verbose=False):

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

                    result = enhance_ocr.image2text(
                        imagefilename, lang, verbose=verbose)

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

class enhance_ocr_descew(object):

    # how to find uris which are not enriched yet?
    # (if not enhanced on indexing but later)

    # this plugin needs to read the field id as a parameters to enrich unenriched docs
    fields = ['id', 'content_type_ss']

    # query to find documents, that were not enriched by this plugin yet
    # (since we marked documents which were OCRd with ocr_b = true
    query = "content_type_ss: image\/* AND NOT enhance_ocr_descew_b:true"

    def process(self, parameters={}, data={}):

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        filename = parameters['filename']

        if 'content_type_ss' in data:
            mimetype = data['content_type_ss']
        else:
            mimetype = parameters['content_type_ss']

        # if connector returns a list, use only first value (which is the only entry of the list)
        if isinstance(mimetype, list):
            mimetype = mimetype[0]

        if 'ocr_lang' in parameters:
            lang = parameters['ocr_lang']
        else:
            lang = 'eng'

        if "image" in mimetype.lower():
            if verbose:
                print(
                    "Mimetype seems image ({}), starting optimized OCR".format(mimetype))

            ocr_txt = optimized_image2text(filename, lang, verbose)

            if ocr_txt:
                data['ocr_descew_t'] = ocr_txt

            # mark the document to indicate, that it was analyzed by this plugin
            data['enhance_ocr_descew_b'] = "true"

        return parameters, data
