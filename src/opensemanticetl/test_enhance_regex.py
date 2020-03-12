#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest

import enhance_regex

class Test_enhance_regex(unittest.TestCase):

    def test(self):

        enhancer = enhance_regex.enhance_regex()

        parameters = {}
        parameters['verbose'] = True
        parameters['regex_lists'] = ['/etc/opensemanticsearch/regex/iban.tsv']

        data = {}
        data['content_txt'] = "An IBAN DE75512108001245126199 from Germany and GB33BUKB20201555555555 from GB and not 75512108001245126199"

        parameters, data = enhancer.process(data=data, parameters=parameters)

        self.assertTrue('DE75512108001245126199' in data['iban_ss'])
        self.assertTrue('GB33BUKB20201555555555' in data['iban_ss'])

        self.assertFalse('75512108001245126199' in data['iban_ss'])
       
       
if __name__ == '__main__':
    unittest.main()
