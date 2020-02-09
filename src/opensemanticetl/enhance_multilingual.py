#
# Multilinguality
#
# Copy content language specific dynamic fields for language specific analysis like stemming, grammar or synonyms
#
# Language has been detected before by plugin enhance_detect_language using Apache Tika / OpenNLP
#


class enhance_multilingual(object):

    verbose = False

    # languages that are defined in index schema for language specific analysis and used if autodetected as documents language
    languages = ['en', 'fr', 'de', 'es', 'hu', 'pt',
                 'nl', 'ro', 'ru', 'it', 'cz', 'ar', 'fa']
    languages_hunspell = ['hu']

    # languages for language specific analysis even if not the autodetected document language
    languages_force = []
    languages_force_hunspell = []

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        if 'verbose' in parameters:
            self.verbose = parameters['verbose']

        if 'languages' in parameters:
            self.languages = parameters['languages']

        if 'languages_hunspell' in parameters:
            self.languages_hunspell = parameters['languages_hunspell']

        if 'languages_force' in parameters:
            self.languages_force = parameters['languages_force']

        if 'languages_force_hunspell' in parameters:
            self.languages_force_hunspell = parameters['languages_force_hunspell']

        if 'languages_exclude_fields' in parameters:
            self.exclude_fields = parameters['languages_exclude_fields']

        if 'languages_exclude_fields_map' in parameters:
            self.exclude_fields_map = parameters['languages_exclude_fields_map']

        language = data.get('language_s', None)

        #
        # exclude fields like technical metadata
        #
    
        exclude_prefix = []
    
        listfile = open('/etc/opensemanticsearch/blacklist/textanalysis/blacklist-fieldname-prefix')
        for line in listfile:
            line = line.strip()
            if line and not line.startswith("#"):
                exclude_prefix.append(line)
        listfile.close()
    
        # suffixes of non-text fields like nubers
        exclude_suffix = []
    
        listfile = open('/etc/opensemanticsearch/blacklist/textanalysis/blacklist-fieldname-suffix')
        for line in listfile:
            line = line.strip()
            if line and not line.startswith("#"):
                exclude_suffix.append(line)
        listfile.close()
    
        # full fieldnames
        exclude_fields = []
        listfile = open('/etc/opensemanticsearch/blacklist/textanalysis/blacklist-fieldname')
        for line in listfile:
            line = line.strip()
            if line and not line.startswith("#"):
                exclude_fields.append(line)
        listfile.close()
    
        exclude_fields_map = {}

        language_fields = ['_text_']
        language_specific_data = {}

        # language specific analysis for recognized language of document
        # if language support of detected language in index schema
        if language in self.languages:
            language_fields.append("text_txt_" + language)

        if language in self.languages_hunspell:
            language_fields.append("text_txt_hunspell_" + language)

        # fields for language specific analysis by forced languages even if other language or false recognized language
        for language_force in self.languages_force:

            language_field = "text_txt_" + language_force

            if not language_field in language_fields:
                language_fields.append(language_field)

        for language_force in self.languages_force_hunspell:

            language_field = "text_txt_hunspell_" + language_force

            if not language_field in language_fields:
                language_fields.append(language_field)

        # copy each data field to language specific field with suffix _txt_$language
        for fieldname in data:

            exclude = False

            # do not copy excluded fields
            for exclude_field in exclude_fields:
                if fieldname == exclude_field:
                    exclude = True

            for prefix in exclude_prefix:
                if fieldname.startswith(prefix):
                    exclude = True

            for suffix in exclude_suffix:
                if fieldname.endswith(suffix):
                    exclude = True

            if not exclude and data[fieldname]:

                # copy field to default field with added suffixes for language dependent stemming/analysis
                for language_field in language_fields:

                    excluded_by_mapping = False

                    if language_field in exclude_fields_map:
                        if fieldname in exclude_fields_map[language_field]:
                            excluded_by_mapping = True
                            if self.verbose:
                                print("Multilinguality: Excluding field {} to be copied to {} by config of exclude_field_map".format(
                                    fieldname, language_field))

                    if not excluded_by_mapping:
                        if self.verbose:
                            print("Multilinguality: Add {} to {}".format(
                                fieldname, language_field))

                        if not language_field in language_specific_data:
                            language_specific_data[language_field] = []

                        if isinstance(data[fieldname], list):
                            language_specific_data[language_field].extend(
                                data[fieldname])
                        else:
                            language_specific_data[language_field].append(
                                data[fieldname])

        # append language specific fields to data
        for key in language_specific_data:
            data[key] = language_specific_data[key]

        return parameters, data
