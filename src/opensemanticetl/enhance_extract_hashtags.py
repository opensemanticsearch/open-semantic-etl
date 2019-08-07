# Extract text from filename
class enhance_extract_hashtags(object):

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        minimallenght = 3

        if 'content_txt' in data:
            if data['content_txt']:
                data['hashtag_ss'] = [word for word in data['content_txt'].split() if (
                    word.startswith("#") and len(word) > minimallenght)]

        return parameters, data
