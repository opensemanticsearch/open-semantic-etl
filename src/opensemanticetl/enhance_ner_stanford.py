import etl
from nltk.tag.stanford import StanfordNERTagger


#
# Stanford Named Entitiy Recognizer (NER)
#

# Appends classified (Persons, Locations, Organizations) entities (names/words) to mapped facets/fields

class enhance_ner_stanford(object):

    # compound words of same class to multi word entities (result is a split by class changes instead of split on single words/tokens)
    def multi_word_entities(self, entities):

        multi_word_entities = []
        multi_word_entity = ""
        last_entity_class = ""

        i = 0

        for entity, entity_class in entities:

            i += 1

            class_change = False

            # new entity class different from last words which had been joined?
            if last_entity_class:
                if entity_class != last_entity_class:
                    class_change = True

            # if new class add last values to dictionary and begin new multi word entity
            if class_change:
                multi_word_entities.append(
                    (multi_word_entity, last_entity_class))
                multi_word_entity = ""

            # add new word to multi word entity
            if multi_word_entity:
                multi_word_entity += " " + entity
            else:
                multi_word_entity = entity

            # if last entity, no next class change, so add now
            if i == len(entities):
                multi_word_entities.append((multi_word_entity, entity_class))

            last_entity_class = entity_class

        return multi_word_entities

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        if 'stanford_ner_mapping' in parameters:
            mapping = parameters['stanford_ner_mapping']
        else:
            # todo: extend mapping for models with more classes like dates
            mapping = {
                'PERSON': 'person_ss',
                'LOCATION': 'location_ss',
                'ORGANIZATION': 'organization_ss',
                'I-ORG': 'organization_ss',
                'I-PER': 'person_ss',
                'I-LOC': 'location_ss',
                'ORG': 'organization_ss',
                'PER': 'person_ss',
                'LOC': 'location_ss',
                'PERS': 'person_ss',
                'LUG': 'location_ss',
                'MONEY': 'money_ss',
            }

        # default classifier
        classifier = 'english.all.3class.distsim.crf.ser.gz'

        if 'stanford_ner_classifier_default' in parameters:
            classifier = parameters['stanford_ner_classifier_default']

        # set language specific classifier, if configured and document language detected
        if 'stanford_ner_classifiers' in parameters and 'language_s' in data:
            # is a language speciic cassifier there for the detected language?
            if data['language_s'] in parameters['stanford_ner_classifiers']:
                classifier = parameters['stanford_ner_classifiers'][data['language_s']]

        # if standard classifier configured to None and no classifier for detected language, exit the plugin
        if not classifier:
            return parameters, data

        kwargs = {}

        if 'stanford_ner_java_options' in parameters:
            kwargs['java_options'] = parameters['stanford_ner_java_options']

        if 'stanford_ner_path_to_jar' in parameters:
            kwargs['path_to_jar'] = parameters['stanford_ner_path_to_jar']

        analyse_fields = ['title_txt', 'content_txt',
                          'description_txt', 'ocr_t', 'ocr_descew_t']

        text = ''
        for field in analyse_fields:
            if field in data:
                text = "{}{}\n".format(text, data[field])

        # classify/tag with class each word of the content
        st = StanfordNERTagger(classifier, encoding='utf8',
                               verbose=verbose, **kwargs)
        entities = st.tag(text.split())

        # compound words of same class to multi word entities (result is a split by class changes instead of split on single words/tokens)
        entities = self.multi_word_entities(entities)

        # if class of entity is mapped to a facet/field, append the entity to this facet/field
        for entity, entity_class in entities:

            if entity_class in mapping:

                if verbose:
                    print("NER classified word(s)/name {} to {}. Appending to mapped facet {}".format(
                        entity, entity_class, mapping[entity_class]))

                etl.append(data, mapping[entity_class], entity)

            else:
                if verbose:
                    print("Since Named Entity Recognition (NER) class {} not mapped to a field/facet, ignore entity/word(s): {}".format(entity_class, entity))

        # mark the document, that it was analyzed by this plugin yet
        data['enhance_ner_stanford_b'] = "true"

        return parameters, data
