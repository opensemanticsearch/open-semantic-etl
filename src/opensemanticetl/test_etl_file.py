#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest

from etl_file import Connector_File

class Test_ETL_file(unittest.TestCase):

    def test_pdf(self):

        etl_file = Connector_File()

        # run ETL of test.pdf with configured plugins and PDF OCR (result of etl_file.py)
        parameters, data = etl_file.index_file(filename='test/test.pdf', additional_plugins=['enhance_pdf_ocr'])

        # check extracted content type
        self.assertEqual(data['content_type_ss'], 'application/pdf')

        # check content type group which is mapped to this content type (result of plugin enhance_contenttype_group.py)
        self.assertEqual(data['content_type_group_ss'], ['Text document'])

        # check extracted title (result of plugin enhance_extract_text_tika_server.py)
        self.assertEqual(data['title_txt'], 'TestPDFtitle')

        # check extracted content of PDF text (result of plugin enhance_extract_text_tika_server.py)
        self.assertTrue('TestPDFContent1 on TestPDFPage1' in data['content_txt'])
        self.assertTrue('TestPDFContent2 on TestPDFPage2' in data['content_txt'])

        # check OCR of embedded images in PDF (result of plugin enhance_pdf_ocr.py)
        self.assertTrue('TestPDFOCRImage1Content1' in data['ocr_t'])
        self.assertTrue('TestPDFOCRImage1Content2' in data['ocr_t'])
        self.assertTrue('TestPDFOCRImage2Content1' in data['ocr_t'])
        self.assertTrue('TestPDFOCRImage2Content2' in data['ocr_t'])
       

if __name__ == '__main__':
    unittest.main()

