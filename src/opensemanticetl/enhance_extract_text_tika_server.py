import os
import tempfile
import sys
import time
import requests


def in_parsers(parser, parsers):

    for value in parsers:
        if isinstance(value, list):
            for subvalue in value:
                if subvalue == parser:
                    return True
        else:
            if value == parser:
                return True

    return False


# Extract text from file(name)
class enhance_extract_text_tika_server(object):

    mapping = {
        'Content-Type': 'content_type_ss',
        'dc:creator': 'author_ss',
        'Content-Encoding': 'Content-Encoding_ss',
        'X-TIKA:Parsed-By': 'X-Parsed-By_ss',
        'dc:title': 'title_txt',
        'dc:subject': 'subject_ss',
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
        
        do_ocr_pdf_tika = parameters.get('ocr_pdf_tika', True)
        do_ocr_pdf = False
        if 'plugins' in parameters:
            if 'enhance_pdf_ocr' in parameters['plugins'] and do_ocr_pdf_tika:
                do_ocr_pdf = True

        # if only OCR for PDF enabled (enhance_pdf_ocr as fallback and OCR by tika enabled) but not OCR for image files,
        # run OCR only if file ending .pdf so disabled OCR for other file types
        if do_ocr_pdf and not do_ocr:
            contenttype = data.get('content_type_ss', None)
            if isinstance(contenttype, list):
                contenttype = contenttype[0]

            if contenttype == 'application/pdf' or filename.lower().endswith('.pdf'):
                do_ocr_pdf = True
            else:
                do_ocr_pdf = False

        if 'ocr_lang' in parameters:
            headers['X-Tika-OCRLanguage'] = parameters['ocr_lang']
        
        if do_ocr or do_ocr_pdf:

            # increase Tikas Tesseract timeout
            headers['X-Tika-OCRTimeout'] = '1000'

            # OCR embeded images in PDF, if not disabled or has to be done by other plugin
            if do_ocr_pdf:
                headers['X-Tika-PDFextractInlineImages'] = 'true'

            # if OCR cache dir enabled, use tesseract cli wrapper with OCR cache
            ocr_cache = parameters.get('ocr_cache')
            if ocr_cache:
                if os.getenv('OPEN_SEMANTIC_ETL_TIKA_SERVER_CACHE'):
                    tika_server = os.getenv('OPEN_SEMANTIC_ETL_TIKA_SERVER_CACHE')
                elif 'tika_server_cache' in parameters:
                    tika_server = parameters['tika_server_cache']

            # set OCR status in indexed document
            data['etl_enhance_extract_text_tika_server_ocr_enabled_b'] = True
            # OCR is enabled, so was done by this Tika call, no images left to OCR
            data['etl_count_images_yet_no_ocr_i'] = 0
        
        else:
            # OCR (yet) disabled, so use the Tika instance using the fake tesseract so we only get OCR results if in cache
            # else we get OCR status [Image (No OCR yet)] in content, so we know that there are images to OCR for later steps

            if os.getenv('OPEN_SEMANTIC_ETL_TIKA_SERVER_FAKECACHE'):
                tika_server = os.getenv('OPEN_SEMANTIC_ETL_TIKA_SERVER_FAKECACHE')
            elif 'tika_server_fakecache' in parameters:
                tika_server = parameters['tika_server_fakecache']

            headers['X-Tika-PDFextractInlineImages'] = 'true'

            # set OCR status in indexed document, so next stage knows that yet no OCR
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
                    filename=filename,
                    serverEndpoint=tika_server,
                    headers=headers,
                    requestOptions={'timeout': 60000})

                no_connection = False

            except requests.exceptions.ConnectionError as e:
                retries += 1
                sys.stderr.write(
                    "Connection to Tika server (will retry in {} seconds) failed. Exception: {}\n".format(retrytime, e))

        if parsed['content']:
            data['content_txt'] = parsed['content']

        tika_exception = False
        for tika_field in parsed["metadata"]:

            # there is a field name with exceptions, so copy fieldname to failed plugins
            if 'exception' in tika_field.lower():
                tika_exception = True
                parameters['etl_tika_exception'] = True
                if 'etl_error_plugins_ss' not in data:
                    data['etl_error_plugins_ss'] = []
                data['etl_error_plugins_ss'].append(tika_field)

            # copy Tika fields to (mapped) data fields
            if tika_field in self.mapping:
                data[self.mapping[tika_field]] = parsed['metadata'][tika_field]
            else:
                data[tika_field + '_ss'] = parsed['metadata'][tika_field]

        #
        # anaylze and (re)set OCR status to prevent (re)process unnecessary tasks of later stage(s)
        #
        contenttype = data.get('content_type_ss', None)
        if isinstance(contenttype, list):
            contenttype = contenttype[0]

        ocr_status_known = False

        # file was PDF and OCR for PDF enabled, so we know status
        if do_ocr_pdf:
            ocr_status_known = True

        # all OCR cases enabled, so we know status
        if do_ocr and do_ocr_pdf:
            ocr_status_known = True

        # if no kind of OCR done now, we know status because fake tesseract wrapper
        if not do_ocr and not do_ocr_pdf:
            ocr_status_known = True
        
        # if OCR for images done but content type is PDF and OCR of PDF by Tika is disabled
        # (because using other plugin for that) we do not know status for PDF,
        # since Tika runned without inline OCR for PDF
        if do_ocr and not do_ocr_pdf:
            if not contenttype == 'application/pdf':
                ocr_status_known = True

        if ocr_status_known:
            
            # Tika made an tesseract OCR call (if OCR (yet) off, by fake Tesseract CLI wrapper)
            # so there is really something to OCR?
            if not in_parsers('org.apache.tika.parser.ocr.TesseractOCRParser', data['X-TIKA:Parsed-By_ss']):
                # since Tika did not call (fake or cached) tesseract (wrapper), nothing to OCR in this file,
    
                if verbose:
                    print('Tika OCR parser not used, so nothing to OCR in later stages, too')
                
                # so set all OCR plugin status and OCR configs to done,
                # so filter_file_not_modifield in later stage task will prevent reprocessing
                # because of only this yet not runned plugins or OCR configs
                data['etl_enhance_extract_text_tika_server_ocr_enabled_b'] = True
                data['etl_count_images_yet_no_ocr_i'] = 0
    
                if not tika_exception:
                    parameters['etl_nothing_for_ocr'] = True
                    data['etl_enhance_ocr_descew_b'] = True
                    data['etl_enhance_pdf_ocr_b'] = True
    
            else:
                # OCR parser used by Tika, so there was something to OCR
    
                # If in this case the fake tesseract wrapper could get all results from cache,
                # no additional Tika-Server run with OCR enabled needed
                # So set Tika-Server OCR status of tika-server to done
    
                if not do_ocr and 'content_txt' in data:
    
                    if verbose:
                        print("Tika OCR parser was used, so there is something to OCR")
    
                    # how many images yet not OCRd because no result from cache
                    # so we got fake OCR result "[Image (no OCR yet)]"
                    count_images_yet_no_ocr = data['content_txt'].count('[Image (no OCR yet)]')
                    data['etl_count_images_yet_no_ocr_i'] = count_images_yet_no_ocr
    
                    # got all Tika-Server Tesseract OCR results from cache,
                    # so no additional OCR tasks for later stage
                    if count_images_yet_no_ocr == 0:
                        if verbose:
                            print('But could get all OCR results in this stage from OCR cache')
                        # therefore set status like OCR related config
                        # yet runned, so on next stage filter_file_not_modified
                        # wont process document again only because of OCR
                        # (but not reset status of other plugins,
                        # since maybe additional image in changed file)
                        data['etl_enhance_extract_text_tika_server_ocr_enabled_b'] = True
                        data['etl_count_images_yet_no_ocr_i'] = 0

                        # if not a (maybe changed) PDF, set enhance_pdf_ocr to done, too,
                        # so no reprocessing because this additional plugin on later stage
                        if not contenttype == 'application/pdf':
                            data['etl_enhance_pdf_ocr_b'] = True

        tika_log_file = tika_log_path + os.path.sep + 'tika.log'
        if os.path.isfile(tika_log_file):
            os.remove(tika_log_file)

        os.rmdir(tika_log_path)

        return parameters, data
