#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import etl

#
# extract email addresses
#

class enhance_extract_email(object):
    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        # todo: use all data fields for analysis
        text = ''
        if 'content_txt' in data:
            text = data['content_txt']

        for match in re.finditer('[\w\.-]+@[\w\.-]+', text, re.IGNORECASE):
            value = match.group(0)
            etl.append(data, 'email_ss', value)


        # if extracted email addresses from data, do further analysis for separated specialized facets
        if 'email_ss' in data:

            # extract email adresses of sender (from)
            for match in re.finditer('From: (.* )?([\w\.-]+@[\w\.-]+)', text, re.IGNORECASE):
                value = match.group(2)
                etl.append(data, 'Message-From_ss', value)

            # extract email adresses (to)
            for match in re.finditer('To: (.* )?([\w\.-]+@[\w\.-]+)', text, re.IGNORECASE):
                value = match.group(2)
                etl.append(data, 'Message-To_ss', value)

            # extract the domain part from all emailadresses to facet email domains
            data['email_domain_ss'] = []
            emails = data['email_ss']
            if not isinstance(emails, list):
                emails = [emails]

            for email in emails:
                domain = email.split('@')[1]
                etl.append(data, 'email_domain_ss', domain)

        return parameters, data
