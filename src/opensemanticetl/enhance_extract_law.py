#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import etl_plugin_core

#
# extract law codes
#

class enhance_extract_law(etl_plugin_core.Plugin):
    
    def process(self, parameters=None, data=None):
        
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        clause_prefixes = [
            '§',
            'Article',
            'Artikel',
            'Art',
            'Section',
            'Sec',
        ]

        clause_subsections = [
            'Abschnitt',
            'Absatz',
            'Abs',
            'Sentence',
            'Satz',
            'S',
            'Halbsatz',
            'Number',
            'Nummer',
            'Nr',
            'Buchstabe',
        ]

        text = etl_plugin_core.get_text(data)


        clauses = []

        rule = '(' + '|'.join(clause_prefixes) + ')\W*((\d+\W\w(\W|\b))|(\d+\w?))(\W?(' + '|'.join(clause_subsections) + ')\W*(\d+\w?|\w(\W|\b)))*'
        for match in re.finditer(rule, text, re.IGNORECASE):
            clause = match.group(0)

            clause = clause.strip()

            clauses.append(clause)

            # if "§123" normalize to "§ 123"
            if clause[0] == '§' and not clause[1] == ' ':
                clause = '§ ' + clause[1:]

            etl_plugin_core.append(data, 'law_clause_ss', clause)

        code_matchtexts = etl_plugin_core.get_all_matchtexts(data.get('Code_of_law_ss_matchtext_ss', []))
        code_matchtexts_with_clause = []

        preflabels = {}
        if 'Code_of_law_ss_preflabel_and_uri_ss' in data:
            preflabels = etl_plugin_core.get_preflabels(data['Code_of_law_ss_preflabel_and_uri_ss'])

        if len(clauses)>0 and len(code_matchtexts)>0:

            text = text.replace("\n", " ")

            for code_match_id in code_matchtexts:

                #get only matchtext (without ID/URI of matching entity)
                for code_matchtext in code_matchtexts[code_match_id]:
    
                    for clause in clauses:
                        if clause + " " + code_matchtext in text or code_matchtext + " " + clause in text:
                            
                            code_matchtexts_with_clause.append(code_matchtext)
                            
                            # if "§123" normalize to "§ 123"
                            if clause[0] == '§' and not clause[1] == ' ':
                                clause = '§ ' + clause[1:]
    
                            law_code_preflabel = code_match_id
                            if code_match_id in preflabels:
                                law_code_clause_normalized = clause + " " + preflabels[code_match_id]
                            else:
                                law_code_clause_normalized = clause + " " + code_match_id
     
                            etl_plugin_core.append(data, 'law_code_clause_ss', law_code_clause_normalized)

        if len(code_matchtexts)>0:
            
            blacklist = []
            listfile = open('/etc/opensemanticsearch/blacklist/enhance_extract_law/blacklist-lawcode-if-no-clause')
            for line in listfile:
                line = line.strip()
                if line and not line.startswith("#"):
                    blacklist.append(line)
            listfile.close()

            if not isinstance(data['Code_of_law_ss_matchtext_ss'], list):
                data['Code_of_law_ss_matchtext_ss'] = [data['Code_of_law_ss_matchtext_ss']]

            blacklisted_code_ids = []
            for code_match_id in code_matchtexts:
                for code_matchtext in code_matchtexts[code_match_id]:
                    if code_matchtext in blacklist:
                        if code_matchtext not in code_matchtexts_with_clause:
                            blacklisted_code_ids.append(code_match_id)
                            data['Code_of_law_ss_matchtext_ss'].remove(code_match_id + "\t" + code_matchtext)

            code_matchtexts = etl_plugin_core.get_all_matchtexts(data.get('Code_of_law_ss_matchtext_ss', []))

            if not isinstance(data['Code_of_law_ss'], list):
                data['Code_of_law_ss'] = [data['Code_of_law_ss']]
            if not isinstance(data['Code_of_law_ss_preflabel_and_uri_ss'], list):
                data['Code_of_law_ss_preflabel_and_uri_ss'] = [data['Code_of_law_ss_preflabel_and_uri_ss']]

            for blacklisted_code_id in blacklisted_code_ids:
                if blacklisted_code_id not in code_matchtexts:
                    data['Code_of_law_ss'].remove(preflabels[blacklisted_code_id])
                    data['Code_of_law_ss_preflabel_and_uri_ss'].remove(preflabels[blacklisted_code_id] + ' <' + blacklisted_code_id + '>')

        return parameters, data
