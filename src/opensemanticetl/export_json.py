import json


class export_json(object):

    def __init__(self, config=None):
        if config is None:
            config = {'verbose': False}

        self.config = config

    #
    # Json data
    #

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        # if outputfile write json to file
        if 'outputfile' in parameters:

            import io
            with io.open(parameters['outputfile'], 'w', encoding='utf-8') as f:
                f.write(json.dumps(data, ensure_ascii=False))
        else:  # else print json
            print(json.dumps(data))

        return parameters, data
