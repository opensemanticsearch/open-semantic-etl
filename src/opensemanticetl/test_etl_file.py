#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest
import os

from etl_file import Connector_File
from etl_delete import Delete

class Test_ETL_file(unittest.TestCase):

    def test_pdf_and_ocr_by_tika(self):

        etl_file = Connector_File()
        filename = os.path.dirname(os.path.realpath(__file__)) + '/testdata/test.pdf'

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
        self.assertTrue('TestPDFOCRImage1Content1' in data['content_txt'])
        self.assertTrue('TestPDFOCRImage1Content2' in data['content_txt'])
        self.assertTrue('TestPDFOCRImage2Content1' in data['content_txt'])
        self.assertTrue('TestPDFOCRImage2Content2' in data['content_txt'])

        # OCR done by Tika so in field content_txt, not in OCR plugin field ocr_t
        self.assertFalse('ocr_t' in data)

        # OCR text copied to default search field by plugin enhance_multilingual?
        default_search_field_data = ' '.join(data['_text_'])
        self.assertTrue('TestPDFOCRImage1Content1' in default_search_field_data)
        self.assertTrue('TestPDFOCRImage1Content2' in default_search_field_data)
        self.assertTrue('TestPDFOCRImage2Content1' in default_search_field_data)
        self.assertTrue('TestPDFOCRImage2Content2' in default_search_field_data)

        # check if a Open Semantic ETL plugin threw an exception
        self.assertEqual(data['etl_error_plugins_ss'], [])


    def test_ocr_by_plugin_enhance_pdf_ocr(self):

        etl_file = Connector_File()
        filename = os.path.dirname(os.path.realpath(__file__)) + '/testdata/test.pdf'

        etl_file.config['ocr_pdf_tika'] = False

        # run ETL of test.pdf with configured plugins and PDF OCR (result of etl_file.py)
        parameters, data = etl_file.index_file(filename = filename, additional_plugins=['enhance_pdf_ocr'])

        # delete from search index
        etl_delete = Delete()
        etl_delete.delete(filename)

        # check OCR of embedded images in PDF (result of plugin enhance_pdf_ocr.py)
        self.assertTrue('TestPDFOCRImage1Content1' in data['ocr_t'])
        self.assertTrue('TestPDFOCRImage1Content2' in data['ocr_t'])
        self.assertTrue('TestPDFOCRImage2Content1' in data['ocr_t'])
        self.assertTrue('TestPDFOCRImage2Content2' in data['ocr_t'])

        # OCR text copied to default search field?
        default_search_field_data = ' '.join(data['_text_'])
        self.assertTrue('TestPDFOCRImage1Content1' in default_search_field_data)
        self.assertTrue('TestPDFOCRImage1Content2' in default_search_field_data)
        self.assertTrue('TestPDFOCRImage2Content1' in default_search_field_data)
        self.assertTrue('TestPDFOCRImage2Content2' in default_search_field_data)

        # check if a Open Semantic ETL plugin threw an exception
        self.assertEqual(data['etl_error_plugins_ss'], [])

if __name__ == '__main__':
    unittest.main()
