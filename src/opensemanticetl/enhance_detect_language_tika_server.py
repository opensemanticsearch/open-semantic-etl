import os
import sys
import time
import requests

# Extract text from filename


class enhance_detect_language_tika_server(object):

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        if os.getenv('OPEN_SEMANTIC_ETL_TIKA_SERVER'):
            tika_server = os.getenv('OPEN_SEMANTIC_ETL_TIKA_SERVER')
        elif 'tika_server' in parameters:
            tika_server = parameters['tika_server']
        else:
            tika_server = 'http://localhost:9998'


        uri = tika_server + '/language/string'

        analyse_fields = ['title_txt', 'content_txt',
                          'description_txt', 'ocr_t', 'ocr_descew_t']

        text = ''
        for field in analyse_fields:
            if field in data:
                text = "{}{}\n".format(text, data[field])

        if verbose:
            print("Calling Tika server for language detection from {}".format(uri))

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

                r = requests.put(uri, data=text.encode('utf-8'))

                no_connection = False

            except requests.exceptions.ConnectionError as e:
                retries += 1
                sys.stderr.write(
                    "Connection to Tika server (will retry in {} seconds) failed. Exception: {}\n".format(retrytime, e))

        language = r.content.decode('utf-8')

        if verbose:
            print("Detected language: {}".format(language))

        data['language_s'] = language

        return parameters, data
