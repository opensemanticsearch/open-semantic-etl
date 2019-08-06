#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest

import enhance_extract_text_tika_server

class Test_enhance_extract_text_tika_server(unittest.TestCase):

    def test(self):

        enhancer = enhance_extract_text_tika_server.enhance_extract_text_tika_server()

        parameters = {'filename': 'test/test.pdf'}

        parameters, data = enhancer.process(parameters=parameters)

        # check extracted content type
        self.assertEqual(data['content_type_ss'], 'application/pdf')

        # check extracted title
        self.assertEqual(data['title_txt'], 'TestPDFtitle')

        # check extracted content of PDF text
        self.assertTrue('TestPDFContent1 on TestPDFPage1' in data['content_txt'])
        self.assertTrue('TestPDFContent2 on TestPDFPage2' in data['content_txt'])

       
if __name__ == '__main__':
    unittest.main()

