#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest
import os

import enhance_extract_text_tika_server


class TestEnhanceExtractTextTikaServer(unittest.TestCase):

    def test_text_extraction_pdf(self):

        enhancer = enhance_extract_text_tika_server.enhance_extract_text_tika_server()

        parameters = {'filename': os.path.dirname(os.path.realpath(__file__)) + '/test/test.pdf'}

        parameters, data = enhancer.process(parameters=parameters)

        # check extracted content type
        self.assertTrue(data['content_type_ss'] == 'application/pdf'
                        or sorted(data['content_type_ss']) == ['application/pdf', 'image/jpeg', 'image/png'])

        # check extracted title
        self.assertEqual(data['title_txt'], 'TestPDFtitle')

        # check extracted content of PDF text
        self.assertTrue('TestPDFContent1 on TestPDFPage1' in data['content_txt'])
        self.assertTrue('TestPDFContent2 on TestPDFPage2' in data['content_txt'])

        # check disabled OCR of embedded images in PDF (result of plugin enhance_pdf_ocr.py)
        self.assertFalse('TestPDFOCRImage1Content1' in data['content_txt'])
        self.assertFalse('TestPDFOCRImage1Content2' in data['content_txt'])
        self.assertFalse('TestPDFOCRImage2Content1' in data['content_txt'])
        self.assertFalse('TestPDFOCRImage2Content2' in data['content_txt'])

    def test_text_extraction_pdf_ocr(self):

        enhancer = enhance_extract_text_tika_server.enhance_extract_text_tika_server()

        parameters = {'ocr': True, 'plugins': ['enhance_pdf_ocr'],
                      'filename': os.path.dirname(os.path.realpath(__file__)) + '/test/test.pdf'}

        parameters, data = enhancer.process(parameters=parameters)

        # check extracted content type
        self.assertTrue(sorted(data['content_type_ss']) == ['application/pdf', 'image/jpeg', 'image/png'])

        # check extracted title
        self.assertEqual(data['title_txt'], 'TestPDFtitle')

        # check extracted content of PDF text
        self.assertTrue('TestPDFContent1 on TestPDFPage1' in data['content_txt'])
        self.assertTrue('TestPDFContent2 on TestPDFPage2' in data['content_txt'])

        # check OCR of embedded images in PDF (result of plugin enhance_pdf_ocr.py)
        self.assertTrue('TestPDFOCRImage1Content1' in data['content_txt'])
        self.assertTrue('TestPDFOCRImage1Content2' in data['content_txt'])
        self.assertTrue('TestPDFOCRImage2Content1' in data['content_txt'])
        self.assertTrue('TestPDFOCRImage2Content2' in data['content_txt'])

    def test_ocr_png(self):

        enhancer = enhance_extract_text_tika_server.enhance_extract_text_tika_server()

        parameters = {'ocr': True,
                      'filename': os.path.dirname(os.path.realpath(__file__)) + '/test/Test_OCR_Image1.png'}

        parameters, data = enhancer.process(parameters=parameters)

        # check extracted content type
        self.assertEqual(data['content_type_ss'], 'image/png')

        # check OCR
        self.assertTrue('TestOCRImage1Content1' in data['content_txt'])
        self.assertTrue('TestOCRImage1Content2' in data['content_txt'])

    def test_ocr_jpg(self):

        enhancer = enhance_extract_text_tika_server.enhance_extract_text_tika_server()

        parameters = {'ocr': True,
                      'filename': os.path.dirname(os.path.realpath(__file__)) + '/test/Test_OCR_Image2.jpg'}

        parameters, data = enhancer.process(parameters=parameters)

        # check extracted content type
        self.assertEqual(data['content_type_ss'], 'image/jpeg')

        # check OCR
        self.assertTrue('TestOCRImage2Content1' in data['content_txt'])
        self.assertTrue('TestOCRImage2Content2' in data['content_txt'])

    def test_disabled_ocr_png(self):

        enhancer = enhance_extract_text_tika_server.enhance_extract_text_tika_server()

        parameters = {'ocr': False,
                      'filename': os.path.dirname(os.path.realpath(__file__)) + '/test/Test_OCR_Image1.png'}

        parameters, data = enhancer.process(parameters=parameters)

        if 'content_txt' not in data:
            data['content_txt'] = "Empty"

        # check extracted content type
        self.assertEqual(data['content_type_ss'], 'image/png')

        # check disabled OCR
        self.assertFalse('TestOCRImage1Content1' in data['content_txt'])
        self.assertFalse('TestOCRImage1Content2' in data['content_txt'])

        # check if Fake tesseract wrapper returned status
        self.assertTrue('[Image (no OCR yet)]' in data['content_txt'])
       

if __name__ == '__main__':
    unittest.main()
