#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest
import os

import enhance_extract_text_tika_server

class Test_enhance_extract_text_tika_server(unittest.TestCase):

    def test_text_extraction_pdf(self):

        enhancer = enhance_extract_text_tika_server.enhance_extract_text_tika_server()

        parameters = {'filename': os.path.dirname(os.path.realpath(__file__)) + '/test/test.pdf'}

        parameters, data = enhancer.process(parameters=parameters)

        # check extracted content type
        self.assertEqual(data['content_type_ss'], 'application/pdf')

        # check extracted title
        self.assertEqual(data['title_txt'], 'TestPDFtitle')

        # check extracted content of PDF text
        self.assertTrue('TestPDFContent1 on TestPDFPage1' in data['content_txt'])
        self.assertTrue('TestPDFContent2 on TestPDFPage2' in data['content_txt'])


    def test_ocr_png(self):

        enhancer = enhance_extract_text_tika_server.enhance_extract_text_tika_server()

        parameters = {'ocr': True, 'filename': os.path.dirname(os.path.realpath(__file__)) + '/test/Test_OCR_Image1.png'}

        parameters, data = enhancer.process(parameters=parameters)

        # check extracted content type
        self.assertEqual(data['content_type_ss'], 'image/png')

        # check OCR
        self.assertTrue('TestOCRImage1Content1' in data['content_txt'])
        self.assertTrue('TestOCRImage1Content2' in data['content_txt'])


    def test_ocr_jgg(self):

        enhancer = enhance_extract_text_tika_server.enhance_extract_text_tika_server()

        parameters = {'ocr': True, 'filename': os.path.dirname(os.path.realpath(__file__)) + '/test/Test_OCR_Image2.jpg'}

        parameters, data = enhancer.process(parameters=parameters)

        # check extracted content type
        self.assertEqual(data['content_type_ss'], 'image/jpeg')

        # check OCR
        self.assertTrue('TestOCRImage2Content1' in data['content_txt'])
        self.assertTrue('TestOCRImage2Content2' in data['content_txt'])


    def test_disabled_ocr(self):

        enhancer = enhance_extract_text_tika_server.enhance_extract_text_tika_server()

        parameters = {'ocr': False, 'filename': os.path.dirname(os.path.realpath(__file__)) + '/test/Test_OCR_Image1.png'}

        parameters, data = enhancer.process(parameters=parameters)

        if not 'content_txt' in data:
            data['content_txt'] = "Empty"

        # check extracted content type
        self.assertEqual(data['content_type_ss'], 'image/png')

        # check disabled OCR
        self.assertFalse('TestOCRImage1Content1' in data['content_txt'])
        self.assertFalse('TestOCRImage1Content2' in data['content_txt'])

       
if __name__ == '__main__':
    unittest.main()

