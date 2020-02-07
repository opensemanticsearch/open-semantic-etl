import os
import tempfile
import sys
import time
import requests


# Extract text from file(name)
class enhance_extract_text_tika_server(object):

    mapping = {
        'Content-Type': 'content_type_ss',
        'Author': 'author_ss',
        'Content-Encoding': 'Content-Encoding_ss',
        'title': 'title_txt',
    }

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

        tika_log_path = tempfile.mkdtemp(prefix="tika-python-")
        os.environ['TIKA_LOG_PATH'] = tika_log_path

        os.environ['TIKA_CLIENT_ONLY'] = 'True'

        import tika
        from tika import parser

        tika.TikaClientOnly = True

        if os.getenv('OPEN_SEMANTIC_ETL_TIKA_SERVER'):
            tika_server = os.getenv('OPEN_SEMANTIC_ETL_TIKA_SERVER')
        elif 'tika_server' in parameters:
            tika_server = parameters['tika_server']
        else:
            tika_server = 'http://localhost:9998'

        headers = {}

        do_ocr = parameters.get('ocr', False)

        if 'ocr_lang' in parameters:
            headers['X-Tika-OCRLanguage'] = parameters['ocr_lang']
        
        if do_ocr:

            # if OCR cache dir enabled, use tesseract cli wrapper with OCR cache
            ocr_cache = parameters.get('ocr_cache')
            if ocr_cache:
                headers['X-Tika-OCRTesseractPath'] = '/usr/lib/python3/dist-packages/tesseract_cache/'

            tessdataPath = parameters.get('ocr_tessdataPath')
            if tessdataPath:
                headers['X-Tika-OCRtessdataPath'] = tessdataPath

            # set OCR status in indexed document
            data['etl_enhance_extract_text_tika_server_ocr_enabled_b'] = True
        
        else:
            # OCR (yet) disabled, so set Tikas Tesseract path to tesseract fake so we only get OCR results if in cache
            # else we get OCR status [Image (No OCR yet)] in content, so we know that there are images to OCR for later steps
            headers['X-Tika-PDFextractInlineImages'] = 'true'
            headers['X-Tika-OCRTesseractPath'] = '/usr/lib/python3/dist-packages/tesseract_fake/'

            # set OCR status in indexed document
            data['etl_enhance_extract_text_tika_server_ocr_enabled_b'] = False

        #
        # Parse on Apache Tika Server by python-tika
        #
        if verbose:
            print("Parsing by Tika Server on {} with additional headers {}".format(tika_server, headers))

        retries = 0
        retrytime = 1
        # wait time until next retry will be doubled until reaching maximum of 120 seconds (2 minutes) until next retry
        retrytime_max = 120
        no_connection = True

        while no_connection:
            try:
                if retries > 0:
                    print(
                        'Retrying to connect to Tika server in {} second(s).'.format(retrytime))
                    time.sleep(retrytime)
                    retrytime = retrytime * 2
                    if retrytime > retrytime_max:
                        retrytime = retrytime_max

                parsed = parser.from_file(
                    filename=filename, serverEndpoint=tika_server, headers=headers)
                no_connection = False

            except requests.exceptions.ConnectionError as e:
                retries += 1
                sys.stderr.write(
                    "Connection to Tika server (will retry in {} seconds) failed. Exception: {}\n".format(retrytime, e))

        if parsed['content']:
            data['content_txt'] = parsed['content']

            if do_ocr:
                # OCR is enabled, so was done by this Tika call, no images left to OCR
                data['etl_count_images_yet_no_ocr_i'] = 0
            else:
                count_images = data['content_txt'].count('[Image (no OCR yet)]')
                
                data['etl_count_images_yet_no_ocr_i'] = count_images

                # no additional OCR tasks for later stage, since no images to OCR
                if count_images == 0:
                    # therefore set status like OCR related config and plugins
                    # yet runned, so on next stage filter_file_not_modified
                    # wont process document again only because of OCR
                    data['etl_enhance_extract_text_tika_server_ocr_enabled_b'] = True
                    data['etl_enhance_ocr_descew_b'] = True
                    data['etl_enhance_pdf_ocr_b'] = True
                    
        # copy Tika fields to (mapped) data fields
        for tika_field in parsed["metadata"]:
            if tika_field in self.mapping:
                data[self.mapping[tika_field]] = parsed['metadata'][tika_field]
            else:
                data[tika_field + '_ss'] = parsed['metadata'][tika_field]

        tika_log_file = tika_log_path + os.path.sep + 'tika.log'
        if os.path.isfile(tika_log_file):
            os.remove(tika_log_file)

        os.rmdir(tika_log_path)

        return parameters, data
