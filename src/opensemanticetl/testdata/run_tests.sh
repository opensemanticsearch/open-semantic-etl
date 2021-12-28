#!/bin/sh

cd /usr/lib/python3/dist-packages/opensemanticetl

python3 -m unittest \
 test_enhance_extract_email \
 test_enhance_mapping_id \
 test_enhance_path
