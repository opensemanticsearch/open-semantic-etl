#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest

import enhance_mapping_id

class Test_enhance_mapping_id(unittest.TestCase):

    def test(self):

        enhancer = enhance_mapping_id.enhance_mapping_id()

        mappings = {
                       "/": "file:///",
                       "/testdir1/": "file:///deep1testdir1/",
                       "/testdir1/testdir2/": "file:///deep2testdir1/deep2testdir2/",
        }
        
        docid = '/test'
        parameters, data = enhancer.process(parameters={'id': docid, 'mappings': mappings})
        self.assertEqual(parameters['id'], 'file:///test')

        docid = '/testdir1/test'
        parameters, data = enhancer.process(parameters={'id': docid, 'mappings': mappings})
        self.assertEqual(parameters['id'], 'file:///deep1testdir1/test')

        docid = '/testdir1/testdir2/test'
        parameters, data = enhancer.process(parameters={'id': docid, 'mappings': mappings})
        self.assertEqual(parameters['id'], 'file:///deep2testdir1/deep2testdir2/test')


    def test_reverse(self):

        mappings = {
                       "/": "file:///",
                       "/testdir1/": "file:///deep1testdir1/",
                       "/testdir1/testdir2/": "file:///deep2testdir1/deep2testdir2/",
        }
        
        docid = 'file:///test'
        reversed_value = enhance_mapping_id.mapping_reverse (docid, mappings)
        self.assertEqual(reversed_value, '/test')

        docid = 'file:///deep1testdir1/test'
        reversed_value = enhance_mapping_id.mapping_reverse (docid, mappings)
        self.assertEqual(reversed_value, '/testdir1/test')

        docid = 'file:///deep2testdir1/deep2testdir2/test'
        reversed_value = enhance_mapping_id.mapping_reverse (docid, mappings)
        self.assertEqual(reversed_value, '/testdir1/testdir2/test')

       
if __name__ == '__main__':
    unittest.main()

