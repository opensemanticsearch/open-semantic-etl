#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest
import os

import enhance_extract_text_tika_server

class TestEnhanceExtractTextTikaServer(unittest.TestCase):

    # delete OCR cache entries for the images used in this test class
    def delete_ocr_cache_entries(self):
        filenames = [
            '/var/cache/tesseract/eng-4c6bf51d4455e1cb58b7d8dd20fb8846f15a3d2c884dc8859802ed689f74ae7a-e96c4b1545a83d86d05f7fbb12ade96d.txt',
            '/var/cache/tesseract/eng-526959d31f4e6b1947bb00c3a02959ef008ce19b9487d95b3df0656159f55a7a-e96c4b1545a83d86d05f7fbb12ade96d.txt',
            '/var/cache/tesseract/eng-c93c49c9dfc9764a4307c2757eb378b2d8cd00f3007ac450605b83f23ecda900-e96c4b1545a83d86d05f7fbb12ade96d.txt',
            '/var/cache/tesseract/eng-ebce8ee4ea7d3d24fe9384212d944adeb58e8f18be15ec06103454f7eade70f5-e96c4b1545a83d86d05f7fbb12ade96d.txt'
        ]
        for filename in filenames:
            if os.path.exists(filename):
                os.remove(filename)

    def setUp(self):
        self.delete_ocr_cache_entries()
    def tearDown(self):
        self.delete_ocr_cache_entries()

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

        # check disabled OCR of embedded images in PDF
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

        # check OCR of embedded images in PDF
        self.assertTrue('TestPDFOCRImage1Content1' in data['content_txt'])
        self.assertTrue('TestPDFOCRImage1Content2' in data['content_txt'])
        self.assertTrue('TestPDFOCRImage2Content1' in data['content_txt'])
        self.assertTrue('TestPDFOCRImage2Content2' in data['content_txt'])

    def test_text_extraction_pdf_ocr_cache(self):

        # add text (changed for this test) to ocr cache, so we can prove that the cache was used
        file = open('/var/cache/tesseract/eng-c93c49c9dfc9764a4307c2757eb378b2d8cd00f3007ac450605b83f23ecda900-e96c4b1545a83d86d05f7fbb12ade96d.txt', "w")
        file.write("TestPDFOCRCacheImage1Content1\n\nTestPDFOCRCacheImage1Content2")
        file.close()

        file = open('/var/cache/tesseract/eng-526959d31f4e6b1947bb00c3a02959ef008ce19b9487d95b3df0656159f55a7a-e96c4b1545a83d86d05f7fbb12ade96d.txt', "w")
        file.write("TestPDFOCRCacheImage2Content1\n\nTestPDFOCRCacheImage2Content2")
        file.close()

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

        # check OCR of embedded images in PDF
        self.assertTrue('TestPDFOCRCacheImage1Content1' in data['content_txt'])
        self.assertTrue('TestPDFOCRCacheImage1Content2' in data['content_txt'])
        self.assertTrue('TestPDFOCRCacheImage2Content1' in data['content_txt'])
        self.assertTrue('TestPDFOCRCacheImage2Content2' in data['content_txt'])

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

        # check extracted content type
        self.assertEqual(data['content_type_ss'], 'image/png')

        # check disabled OCR
        self.assertFalse('TestOCRImage1Content1' in data['content_txt'])
        self.assertFalse('TestOCRImage1Content2' in data['content_txt'])

        # check if Fake tesseract wrapper returned status
        self.assertTrue('[Image (no OCR yet)]' in data['content_txt'])
       

if __name__ == '__main__':
    unittest.main()
