#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest

import enhance_detect_language_tika_server

class Test_enhance_detect_language_tika_server(unittest.TestCase):

    def test(self):

        enhancer = enhance_detect_language_tika_server.enhance_detect_language_tika_server()

        # English
        parameters, data = enhancer.process(data={'content_txt': 'This sentence is written in english language.'})
        self.assertEqual(data['language_s'], 'en')

        # German
        parameters, data = enhancer.process(data={'content_txt': 'Dies ist ein Satz in der Sprache Deutsch.'})
        self.assertEqual(data['language_s'], 'de')

       
if __name__ == '__main__':
    unittest.main()

