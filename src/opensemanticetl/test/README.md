Automated tests by unittest
===========================

Automated tests are implemented using the Python library unittest:

https://docs.python.org/3/library/unittest.html

The code for the unit tests is located in the directory "src/opensemanticetl" in files with the prefix "test_".

Some files with testdata like test documents (see section "Testdata") are located in the subdirectory "test".


Run all tests
=============

Within the directory "src/opensemanticetl" call

python3 -m unittest

to run all available tests for all modules and plugins.


Run tests for a single plugin
=============================

You can run only the tests for a single plugin you currently work on:

For example to test only the Tika plugin for text extraction ("enhance_extract_text_tika_server.py"), call

python3 -m unittest test_enhance_extract_text_tika_server


Testdata
========

Test documents located in subdirectory "test":

test.pdf
--------
A test PDF with two pages with text content to test content extraction and embedded images to test OCR.
