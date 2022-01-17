import os.path

#
# Build and add path facets from filename
#

class enhance_path(object):

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        docid = parameters['id']

        filename_extension = os.path.splitext(docid)[1][1:].lower()
        if filename_extension:
            data['filename_extension_s'] = filename_extension

        if 'facet_path_strip_prefix' in parameters:
            facet_path_strip_prefix = parameters['facet_path_strip_prefix']
        else:
            facet_path_strip_prefix = ['file://', 'http://', 'https://']

        # if begins with unwanted path prefix strip it
        if facet_path_strip_prefix:
            for prefix in facet_path_strip_prefix:
                if docid.startswith(prefix):
                    docid = docid.replace(prefix, '', 1)
                    break

        # replace backslash (i.e. windows filenames) with unix path seperator
        docid = docid.replace("\\", '/')

        # replace # (i.e. uri) with unix path seperator
        docid = docid.replace("#", '/')

        # if more then one /
        docid = docid.replace("//", '/')

        # split paths
        path = docid.split('/')

        # its only a domain
        if (len(path) == 1) or (len(path) == 2 and docid.endswith('/')):
            data['path0_s'] = path[0]

        else:
            # its a path

            # if leeding / on unix paths, split leads to first element empty, so delete it
            if not path[0]:
                del path[0]

            i = 0
            for subpath in path:

                if i == len(path) - 1:
                    # last element, so basename/pure filename without path
                    if subpath:  # if not ending / so empty last part after split
                        data['path_basename_s'] = subpath
                else:
                    # not last path element (=filename), so part of path, not the filename at the end
                    data['path' + str(i) + '_s'] = subpath
                    i += 1

        return parameters, data
