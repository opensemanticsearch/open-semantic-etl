import json
import os
import sys
import requests

from etl import ETL

#
# split text to sentences
#


class enhance_sentence_segmentation(object):

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        if 'id' in data:
            docid = data['id']
        else:
            docid = parameters['id']

        # default classifier
        classifier = 'en_core_web_sm'

        if 'spacy_ner_classifier_default' in parameters:
            classifier = parameters['spacy_ner_classifier_default']

        # set language specific classifier, if configured and document language detected
        if 'spacy_ner_classifiers' in parameters and 'language_s' in data:
            # is a language speciic cassifier there for the detected language?
            if data['language_s'] in parameters['spacy_ner_classifiers']:
                classifier = parameters['spacy_ner_classifiers'][data['language_s']]

                analyse_fields = ['content_txt', 'ocr_t', 'ocr_descew_t']

        text = ''
        for field in analyse_fields:
            if field in data:
                text = "{}{}\n".format(text, data[field])

        # extract sentences from text
        url = "http://localhost:8080/sents"
        if os.getenv('OPEN_SEMANTIC_ETL_SPACY_NER'):
                url = os.getenv('OPEN_SEMANTIC_ETL_SPACY_NER') + '/sents'

        headers = {'content-type': 'application/json'}
        d = {'text': text, 'model': classifier}

        response = requests.post(url, data=json.dumps(d), headers=headers)
        sentences = response.json()

        etl = ETL()

        sentencenumber = 0

        for sentence in sentences:

            sentencenumber += 1

            partdocid = docid + '#sentence' + str(sentencenumber)

            partparameters = parameters.copy()
            partparameters['plugins'] = ['enhance_path', 'enhance_detect_language_tika_server',
                                         'enhance_entity_linking', 'enhance_multilingual']

            if 'enhance_ner_spacy' in parameters['plugins']:
                partparameters['plugins'].append('enhance_ner_spacy')
            if 'enhance_ner_stanford' in parameters['plugins']:
                partparameters['plugins'].append('enhance_ner_stanford')

            sentencedata = {}
            sentencedata['id'] = partdocid

            sentencedata['container_s'] = docid

            if 'author_ss' in data:
                sentencedata['author_ss'] = data['author_ss']

            sentencedata['content_type_group_ss'] = "Sentence"
            sentencedata['content_type_ss'] = "Sentence"
            sentencedata['content_txt'] = sentence

            # index sentence
            try:
                partparameters, sentencedata = etl.process(
                    partparameters, sentencedata)

            except BaseException as e:
                sys.stderr.write(
                    "Exception adding sentence {} : {}".format(sentencenumber, e))

        data['sentences_i'] = sentencenumber

        return parameters, data
