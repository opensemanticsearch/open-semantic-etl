#!/bin/sh

python3 -m unittest discover -s /usr/lib/python3/dist-packages/entity_linking/

python3 -m unittest discover -s /usr/lib/python3/dist-packages/opensemanticetl/
