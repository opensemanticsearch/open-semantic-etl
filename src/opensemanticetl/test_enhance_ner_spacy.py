#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest

import enhance_ner_spacy

class Test_enhance_ner_spacy(unittest.TestCase):

    def test_en(self):

        enhancer = enhance_ner_spacy.enhance_ner_spacy()

        parameters = {'language_s': 'en'}
        data = { 'content_txt': "Some years ago, Mr. Barack Obama, a member of Democratic Party, was president of the USA." }

        parameters, data = enhancer.process(parameters=parameters, data=data)
        print (data)

        self.assertTrue('Barack Obama' in data['person_ss'])
        self.assertTrue('Democratic Party' in data['organization_ss'])
        self.assertTrue('USA' in data['location_ss'])


    def test_de(self):

        enhancer = enhance_ner_spacy.enhance_ner_spacy()

        parameters = {'language_s': 'de'}
        data = { 'content_txt': "Frau Dr. Angela Merkel, Mitglied der CDU, wurde Kanzlerin in Deutschland." }

        parameters, data = enhancer.process(parameters=parameters, data=data)
        print (data)

        self.assertTrue('Angela Merkel' in data['person_ss'])
        self.assertTrue('CDU' in data['organization_ss'])
        self.assertTrue('Deutschland' in data['location_ss'])

       
if __name__ == '__main__':
    unittest.main()

