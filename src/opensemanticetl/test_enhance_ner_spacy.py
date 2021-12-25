#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest

import enhance_ner_spacy

config = {
    'spacy_ner_classifiers': {
        'de': 'de_core_news_sm',
        'en': 'en_core_web_md'
    }
}

class Test_enhance_ner_spacy(unittest.TestCase):

    def test_en(self):

        enhancer = enhance_ner_spacy.enhance_ner_spacy()

        parameters = config.copy()
        data = {
            'language_s': 'en',
            'content_txt': "Some years ago, Mr. Barack Obama, a member of Democratic Party, was president of the USA."
        }

        parameters, data = enhancer.process(parameters=parameters, data=data)

        self.assertTrue('Barack Obama' in data['person_ss'])
        self.assertTrue('Democratic Party' in data['organization_ss'])
        self.assertTrue('USA' in data['location_ss'])


    def test_de(self):

        enhancer = enhance_ner_spacy.enhance_ner_spacy()

        parameters = config.copy()
        data = {
            'language_s': 'de',
            'content_txt': "Der Text ist über Frau Dr. Angela Merkel. Sie ist Mitglied in der CDU. Sie lebt in Deutschland."
        }

        parameters, data = enhancer.process(parameters=parameters, data=data)

        self.assertTrue('Angela Merkel' in data['person_ss'])
        self.assertTrue('CDU' in data['organization_ss'])
        self.assertTrue('Deutschland' in data['location_ss'])

       
if __name__ == '__main__':
    unittest.main()

