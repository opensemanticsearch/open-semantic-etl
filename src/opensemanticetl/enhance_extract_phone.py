#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import etl_plugin_core

#
# normalize phone number (remove all non-numeric chars except leading +)
# so same number is used for aggregations/facet filters, even if written in different formats (with or without space(s) and hyphen(s))
#

def normalize_phonenumber(phone):
    chars = ['+','0','1','2','3','4','5','6','7','8','9']
    phone_normalized = ''
    for char in phone:
        if char in chars:
            # only first +
            if char == '+':
                if not phone_normalized:
                    phone_normalized = '+'
            else:
                phone_normalized += char

    return phone_normalized


#
# extract phone number(s)
#

class enhance_extract_phone(object):
    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        # collect/copy to be analyzed text from all fields
        text = etl_plugin_core.get_text(data=data)

        for match in re.finditer('[\+\(]?[1-9][0-9 .\-\(\)]{8,}[0-9]', text, re.IGNORECASE):
            value = match.group(0)
            etl_plugin_core.append(data, 'phone_ss', value)


        # if extracted phone number(s), normalize to format that can be used for aggregation/filters

        if 'phone_ss' in data:

            phones = data['phone_ss']
            if not isinstance(phones, list):
                phones = [phones]

            for phone in phones:
                phone_normalized = normalize_phonenumber(phone)
                etl_plugin_core.append(data, 'phone_normalized_ss', phone_normalized)

        return parameters, data
