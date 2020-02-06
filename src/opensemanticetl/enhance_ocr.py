from tesseract_cache import tesseract_cache

#
# If image add ocr text
#
class enhance_ocr(object):

    # how to find uris which are not enriched yet?
    # (if not enhanced on indexing but later)

    # this plugin needs to read the field id as a
    # parameters to enrich unenriched docs
    fields = ['id', 'content_type']

    # query to find documents, that were not enriched by this plugin yet
    # (since we marked documents which were OCRd with ocr_b = true
    query = "content_type: image/* AND NOT enhance_ocr_b:true"

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

        if 'content_type_ss' in data:
            mimetype = data['content_type_ss']
        else:
            mimetype = parameters['content_type_ss']

        # if connector returns a list, use only first
        # value (which is the only entry of the list)
        if isinstance(mimetype, list):
            mimetype = mimetype[0]

        if 'ocr_lang' in parameters:
            lang = parameters['ocr_lang']
        else:
            lang = 'eng'

        if "image" in mimetype.lower():
            if verbose:
                print("Mimetype seems image ({}), starting OCR"
                      .format(mimetype))

            ocr_txt = tesseract_cache.get_ocr_text(filename=filename, lang=lang, cache_dir=parameters.get("ocr_cache"), verbose=verbose)

            if ocr_txt:
                data['ocr_t'] = ocr_txt

            # mark the document to indicate
            # that it was analyzed by this plugin
            data['enhance_ocr_b'] = "true"

        return parameters, data
