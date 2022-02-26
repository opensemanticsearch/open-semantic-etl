#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import etl_plugin_core
from numerizer import numerize

#
# extract money
#

class enhance_extract_money(etl_plugin_core.Plugin):

    # todo: all other currency signs from Wikidata
    currency_signs = ['$', '€']

    def process(self, parameters=None, data=None):

        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        text = etl_plugin_core.get_text(data)
        text = text.replace("\n", " ")

        # convert written numbers like "one" and "two million" to integer like "1" and "2000000"
        if 'language_s' in data:
            if data['language_s'] == "en":
                text = numerize(text)

        currencies_escaped = []

        # currency signs
        for currency in self.currency_signs:
            currencies_escaped.append(re.escape(currency))

        # currency labels
        matched_currency_labels = etl_plugin_core.get_all_matchtexts(data.get('currency_ss_matchtext_ss', []))
        for currency_id in matched_currency_labels:
            #get only matchtext (without ID/URI of matching entity)
            for matchtext in matched_currency_labels[currency_id]:
                currencies_escaped.append(re.escape(matchtext))

        regex_part_number = '\d+((\.|\,)\d+)*'
        regex_part_currencies = '(' + '|'.join(currencies_escaped) + ')'

        rule = regex_part_number + '\s?' + regex_part_currencies
        for match in re.finditer(rule, text, re.IGNORECASE):
            etl_plugin_core.append(data, 'money_ss', match.group(0))

        rule = regex_part_currencies + '\s?' + regex_part_number
        for match in re.finditer(rule, text, re.IGNORECASE):
            etl_plugin_core.append(data, 'money_ss', match.group(0))

        return parameters, data
