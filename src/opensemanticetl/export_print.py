import pprint


class export_print(object):

    def __init__(self, config=None):
        if config is None:
            config = {'verbose': False}

        self.config = config

    #
    # Print data
    #

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        pprint.pprint(data)

        return parameters, data
