#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest
import os.path
import enhance_ocr_descew

class Test_enhance_ocr_descew(unittest.TestCase):

    def test(self):

        enhancer = enhance_ocr_descew.enhance_ocr_descew()

        filename = os.path.dirname(os.path.realpath(__file__)) + '/test/Test_OCR_Image1.png'
        parameters = {'filename': filename, 'content_type_ss': 'image/png', 'ocr_cache': '/var/cache/tesseract'}
        data = {}

        parameters, data = enhancer.process(parameters=parameters,data=data)

        self.assertTrue('Image1Content2' in data['ocr_descew_t'])

       
if __name__ == '__main__':
    unittest.main()
