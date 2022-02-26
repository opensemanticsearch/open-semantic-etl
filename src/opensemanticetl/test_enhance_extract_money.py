#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest

from etl import ETL

class Test_enhance_extract_money(unittest.TestCase):

    def test(self):

        etl = ETL()
        etl.config['plugins'] = ['enhance_entity_linking', 'enhance_extract_money']
        etl.config['raise_pluginexception'] = True
        data = {}
        data['content_txt'] = "\n".join([
            "abc $ 123 xyz",
            "abc $ 124,000 xyz",
            "abc 234 $ xyz",
            "abc 235,000 $ xyz",
            "abc 236,99 $ xyz",
            "abc $1234 xyz",
            "abc 2345$ xyz",
            "4444 dollar",
            "44444 USD",
            "444 €",
            "445.000 €",
            "450,99 €",
            "4444 EUR",
            "46.000 EUR",
            "47.000,99 EUR",
            "44,22 EURO",
            "if ambiguous like $ 77 € for more completeness we want to extract both possible variants",
        ])


        parameters, data = etl.process(parameters={'id': 'test_enhance_extract_money'}, data=data)

        self.assertTrue('$ 123' in data['money_ss'])
        self.assertTrue('$ 124,000' in data['money_ss'])
        self.assertTrue('234 $' in data['money_ss'])
        self.assertTrue('235,000 $' in data['money_ss'])
        self.assertTrue('236,99 $' in data['money_ss'])
        self.assertTrue('$1234' in data['money_ss'])
        self.assertTrue('2345$' in data['money_ss'])
        self.assertTrue('4444 dollar' in data['money_ss'])
        self.assertTrue('44444 USD' in data['money_ss'])
        self.assertTrue('444 €' in data['money_ss'])
        self.assertTrue('445.000 €' in data['money_ss'])
        self.assertTrue('450,99 €' in data['money_ss'])
        self.assertTrue('4444 EUR' in data['money_ss'])
        self.assertTrue('46.000 EUR' in data['money_ss'])
        self.assertTrue('47.000,99 EUR' in data['money_ss'])
        self.assertTrue('44,22 EURO' in data['money_ss'])
        self.assertTrue('$ 77' in data['money_ss'])
        self.assertTrue('77 €' in data['money_ss'])

    def test_numerizer(self):

        etl = ETL()
        etl.config['plugins'] = ['enhance_entity_linking', 'enhance_extract_money']
        etl.config['raise_pluginexception'] = True
        data = { 'language_s': 'en' }
        data['content_txt'] = "\n".join([
            "So two million two hundred and fifty thousand and seven $ were given to them",
            "We got twenty one thousand four hundred and seventy three dollars from someone",
        ])

        parameters, data = etl.process(parameters={'id': 'test_enhance_extract_money_numerize'}, data=data)

        self.assertTrue('2250007 $' in data['money_ss'])
        self.assertTrue('21473 dollars' in data['money_ss'])

if __name__ == '__main__':
    unittest.main()

