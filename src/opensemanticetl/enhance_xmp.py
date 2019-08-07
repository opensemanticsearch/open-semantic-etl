import xml.etree.ElementTree as ElementTree
import os.path
import sys


#
# is there a xmp sidecar file?
#

def get_xmp_filename(filename):

    xmpfilename = False

    # some xmp sidecar filenames are based on the original filename without extensions like .jpg or .jpeg
    filenamewithoutextension = '.' . join(filename.split('.')[:-1])

    # check if a xmp sidecar file exists
    if os.path.isfile(filename + ".xmp"):
        xmpfilename = filename + ".xmp"
    elif os.path.isfile(filename + ".XMP"):
        xmpfilename = filename + ".XMP"
    elif os.path.isfile(filenamewithoutextension + ".xmp"):
        xmpfilename = filenamewithoutextension + ".xmp"
    elif os.path.isfile(filenamewithoutextension + ".XMP"):
        xmpfilename = filenamewithoutextension + ".XMP"

    return xmpfilename


# Build path facets from filename

class enhance_xmp(object):
    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        filename = parameters['filename']

        #
        # is there a xmp sidecar file?
        #
        xmpfilename = get_xmp_filename(filename)

        if not xmpfilename:
            if verbose:
                print("No xmp sidecar file")

        #
        # read meta data of the xmp sidecar file (= xml + rdf)
        #
        if xmpfilename:

            creator = False
            headline = False
            creator = False
            location = False
            tags = []

            if verbose:
                print("Reading xmp sidecar file {}".format(xmpfilename))
            try:

                # Parse the xmp file with utf 8 encoding
                parser = ElementTree.XMLParser(encoding="utf-8")
                et = ElementTree.parse(xmpfilename, parser)
                root = et.getroot()

                # get author
                try:
                    creator = root.findtext(
                        ".//{http://purl.org/dc/elements/1.1/}creator")

                    if creator:
                        data['author_ss'] = creator

                except BaseException as e:
                    sys.stderr.write("Exception while parsing creator from xmp {} {}".format(
                        xmpfilename, e.args[0]))

                # get headline
                try:
                    headline = root.findtext(
                        ".//{http://ns.adobe.com/photoshop/1.0/}Headline")

                    if headline:
                        data['title_txt'] = headline

                except BaseException as e:
                    sys.stderr.write("Exception while parsing headline from xmp {} {}".format(
                        xmpfilename, e.args[0]))

                # get location
                try:
                    location = root.findtext(
                        ".//{http://iptc.org/std/Iptc4xmpCore/1.0/xmlns/}Location")

                    if location:

                        if 'locations_ss' in data:
                            data['locations_ss'].append(location)
                        else:
                            data['locations_ss'] = [location]

                except BaseException as e:
                    sys.stderr.write("Exception while parsing location from xmp {} {}".format(
                        xmpfilename, e.args[0]))

                # get tags (named "subject")
                try:
                    for tag in root.findall(".//{http://purl.org/dc/elements/1.1/}subject/{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Bag/{http://www.w3.org/1999/02/22-rdf-syntax-ns#}li"):
                        try:
                            if 'tag_ss' in data:
                                data['tag_ss'].append(tag.text)
                            else:
                                data['tag_ss'] = [tag.text]

                        except BaseException as e:
                            sys.stderr.write("Exception while parsing a tag from xmp {} {}".format(
                                xmpfilename, e.args[0]))
                except BaseException as e:
                    sys.stderr.write("Exception while parsing tags from xmp {} {}".format(
                        xmpfilename, e.args[0]))

            except BaseException as e:
                sys.stderr.write("Exception while parsing xmp {} {}".format(
                    xmpfilename, e.args[0]))

        return parameters, data
