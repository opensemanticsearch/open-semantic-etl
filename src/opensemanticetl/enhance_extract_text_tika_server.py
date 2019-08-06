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

        if 'tika_server' in parameters:
            tika_server = parameters['tika_server']
        else:
            tika_server = 'http://localhost:9998'

        headers = {}
        if 'ocr_lang' in parameters:
            headers = {'X-Tika-OCRLanguage': parameters['ocr_lang']}

        if 'ocr' in parameters:
            if parameters['ocr']:
                data['etl_enhance_extract_text_tika_server_ocr_enabled_b'] = True
            else:
                headers = {'X-Tika-OCRTesseractPath': '/False'}

        #
        # Parse on Apache Tika Server by python-tika
        #
        if verbose:
            print("Parsing by Tika Server on {}".format(tika_server))

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
