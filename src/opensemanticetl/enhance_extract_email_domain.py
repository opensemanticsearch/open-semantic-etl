#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# extract only the domains name from facet email address
#


class enhance_extract_email_domain(object):
    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        if 'email_ss' in data:
            data['email_domain_ss'] = []
            emails = data['email_ss']
            if not isinstance(emails, list):
                emails = [emails]

            for email in emails:
                domain = email.split('@')[1]
                data['email_domain_ss'].append(domain)

        return parameters, data
