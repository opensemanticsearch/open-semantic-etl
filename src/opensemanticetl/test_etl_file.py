#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest
import os

from etl_file import Connector_File
from etl_delete import Delete

class Test_ETL_file(unittest.TestCase):

    def test_pdf(self):

        etl_file = Connector_File()

        filename = os.path.dirname(os.path.realpath(__file__)) + '/test/test.pdf'

        # run ETL of test.pdf with configured plugins and PDF OCR (result of etl_file.py)
        parameters, data = etl_file.index_file(filename = filename, additional_plugins=['enhance_pdf_ocr'])

        # delete from search index
        etl_delete = Delete()
        etl_delete.delete(filename)

        # check extracted content type
        self.assertTrue(data['content_type_ss'] == 'application/pdf' or sorted(data['content_type_ss']) == ['application/pdf', 'image/jpeg', 'image/png'])

        # check content type group which is mapped to this content type (result of plugin enhance_contenttype_group.py)
        self.assertTrue(data['content_type_group_ss'] == ['Text document'] or sorted(data['content_type_group_ss']) == ['Image', 'Text document'])

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

        # check if a plugin threw an exception
        self.assertEqual(len(data['etl_error_plugins_ss']), 0)

if __name__ == '__main__':
    unittest.main()

