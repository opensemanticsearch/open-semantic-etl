#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest

from etl import ETL

class Test_enhance_extract_law(unittest.TestCase):

    def test(self):

        etl = ETL()
        etl.config['plugins'] = ['enhance_entity_linking', 'enhance_extract_law']
        etl.config['raise_pluginexception'] = True
        data = {}
        data['content_txt'] = "\n".join([
            "abc § 888 xyz"
            "abc § 987 b xyz"
            "§12",
            "§ 123",
            "§345a",
            "§456 b",
            "§ 567 c",
            "BGB § 153 Abs. 1 Satz 2",
            "§ 52 Absatz 1 Nummer 2 Buchstabe c STGB",
            "§ 444 CC"
        ])


        # run ETL of test.pdf with configured plugins and PDF OCR (result of etl_file.py)
        parameters, data = etl.process(parameters={'id': 'test_enhance_extract_law'}, data=data)

        self.assertTrue('§ 888' in data['law_clause_ss'])
        self.assertTrue('§ 987 b' in data['law_clause_ss'])
        self.assertTrue('§ 12' in data['law_clause_ss'])
        self.assertTrue('§ 123' in data['law_clause_ss'])
        self.assertTrue('§ 345a' in data['law_clause_ss'])
        self.assertTrue('§ 456 b' in data['law_clause_ss'])
        self.assertTrue('§ 567 c' in data['law_clause_ss'])

        self.assertTrue('§ 153 Abs. 1 Satz 2' in data['law_clause_ss'])
        self.assertTrue('§ 52 Absatz 1 Nummer 2 Buchstabe c' in data['law_clause_ss'])
        
        self.assertTrue('Strafgesetzbuch' in data['Code_of_law_ss'])
        self.assertTrue('Bürgerliches Gesetzbuch' in data['Code_of_law_ss'])
        
        self.assertTrue('Swiss Civil Code' in data['Code_of_law_ss'])


    def test_blacklist(self):

        etl = ETL()
        etl.config['plugins'] = ['enhance_entity_linking', 'enhance_extract_law']
        etl.config['raise_pluginexception'] = True
        data = {}
        data['content_txt'] = "\n".join([
            "No clause for law code alias CC"
        ])

        parameters, data = etl.process(parameters={'id': 'test_enhance_extract_law'}, data=data)
        
        self.assertFalse('Swiss Civil Code' in data['Code_of_law_ss'])

        data['content_txt'] = "\n".join([
            "No clause for blacklisted law code alias CC but not blacklisted label of this alias: Swiss Civil Code"
        ])

        parameters, data = etl.process(parameters={'id': 'test_enhance_extract_law'}, data=data)
        
        self.assertTrue('Swiss Civil Code' in data['Code_of_law_ss'])


if __name__ == '__main__':
    unittest.main()

