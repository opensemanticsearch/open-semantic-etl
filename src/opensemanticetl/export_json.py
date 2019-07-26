import json


class export_json(object):

    def __init__(self, config={'verbose': False}):

        self.config = config

    #
    # Json data
    #

    def process(self, parameters={}, data={}):

        # if outputfile write json to file
        if 'outputfile' in parameters:

            import io
            with io.open(parameters['outputfile'], 'w', encoding='utf-8') as f:
                f.write(unicode(json.dumps(data, ensure_ascii=False)))
        else:  # else print json
            print(json.dumps(data))

        return parameters, data
