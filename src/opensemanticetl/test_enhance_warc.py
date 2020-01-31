#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest
import os

from etl_file import Connector_File
from etl_delete import Delete
from export_solr import export_solr

class Test_enhance_warc(unittest.TestCase):

    def test_warc(self):

        etl_file = Connector_File()
        exporter = export_solr()

        filename = os.path.dirname(os.path.realpath(__file__)) + '/test/example.warc'

        # run ETL of example.warc with configured plugins and warc extractor
        parameters, data = etl_file.index_file(filename = filename)

        contained_doc_id = 'http://example.com/<urn:uuid:a9c51e3e-0221-11e7-bf66-0242ac120005>'
        fields = ['id', 'title_txt', 'content_type_ss', 'content_txt']

        data = exporter.get_data(contained_doc_id, fields)

        # delete from search index
        etl_delete = Delete()
        etl_delete.delete(filename)
        etl_delete.delete(contained_doc_id)

        self.assertEqual(data['title_txt'], ['Example Domain'])

        self.assertEqual(data['content_type_ss'], ['text/html; charset=UTF-8'])

        self.assertTrue('This domain is established to be used for illustrative examples in documents.' in data['content_txt'][0])

if __name__ == '__main__':
    unittest.main()
