import etl_plugin_core

# Extract text from filename
class enhance_extract_hashtags(object):

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        minimallenght = 3

        # collect/copy to be analyzed text from all fields
        text = etl_plugin_core.get_text(data=data)

        data['hashtag_ss'] = [word for word in text.split() if (
            word.startswith("#") and len(word) > minimallenght)]

        return parameters, data
