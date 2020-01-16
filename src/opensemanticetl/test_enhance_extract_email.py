#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest

import enhance_extract_email

class Test_enhance_extract_email(unittest.TestCase):

    def test(self):

        enhancer = enhance_extract_email.enhance_extract_email()

        data = {}
        data['content_txt'] = 'one@localnet.localdomain at begin and two@localnet2.localdomain in the middle and last three@localnet3.localdomain'

        parameters, data = enhancer.process(data=data)

        self.assertTrue('one@localnet.localdomain' in data['email_ss'])
        self.assertTrue('two@localnet2.localdomain' in data['email_ss'])
        self.assertTrue('three@localnet3.localdomain' in data['email_ss'])

        self.assertTrue('localnet.localdomain' in data['email_domain_ss'])
        self.assertTrue('localnet2.localdomain' in data['email_domain_ss'])
        self.assertTrue('localnet3.localdomain' in data['email_domain_ss'])
       
if __name__ == '__main__':
    unittest.main()

