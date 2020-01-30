#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest
import os

import enhance_pdf_ocr

class Test_enhance_pdf_ocr(unittest.TestCase):

    # check OCR of embedded images in PDF
    def test_pdf_ocr(self):

        enhancer = enhance_pdf_ocr.enhance_pdf_ocr()

        parameters = {'filename': os.path.dirname(os.path.realpath(__file__)) + '/test/test.pdf', 'content_type_ss': 'application/pdf','plugins':[]}

        parameters, data = enhancer.process(parameters=parameters)

        self.assertTrue('TestPDFOCRImage1Content1' in data['ocr_t'])
        self.assertTrue('TestPDFOCRImage1Content2' in data['ocr_t'])
        self.assertTrue('TestPDFOCRImage2Content1' in data['ocr_t'])
        self.assertTrue('TestPDFOCRImage2Content2' in data['ocr_t'])

    # check OCR after descewing with Scantailor of embedded images in PDF
    def test_pdf_ocr_descew(self):

        enhancer = enhance_pdf_ocr.enhance_pdf_ocr()

        parameters = {'filename': 'test/test.pdf', 'content_type_ss': 'application/pdf','plugins':['enhance_ocr_descew']}

        parameters, data = enhancer.process(parameters=parameters)
        self.assertTrue('TestPDFOCRImage1' in data['ocr_descew_t'])
        self.assertTrue('TestPDFOCRImage2' in data['ocr_descew_t'])

       
if __name__ == '__main__':
    unittest.main()

