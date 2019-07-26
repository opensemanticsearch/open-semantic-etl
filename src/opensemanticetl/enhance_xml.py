import xml.etree.ElementTree as ElementTree
import os.path
import sys


class enhance_xml(object):

    def elements2data(self, element, data, path="xml"):

        path += "/" + element.tag

        fieldname = path + '_ss'

        text = element.text.strip()

        if text:
            if fieldname in data:
                data[fieldname].append(text)
            else:
                data[fieldname] = [text]

        for child in element:
            data = self.elements2data(element=child, path=path, data=data)

        return data

    # get xml filename by mapping configuration
    def get_xml_filename(self, filename, mapping):

        dirname = os.path.dirname(filename)
        basename = os.path.basename(filename)

        xmlfilename = mapping

        xmlfilename = xmlfilename.replace('%DIRNAME%', dirname)
        xmlfilename = xmlfilename.replace('%BASENAME%', dirname)

        if not os.path.isfile(xmlfilename):
            xmlfilename = False

        return xmlfilename

    def process(self, parameters={}, data={}):

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        filename = parameters['filename']

        mapping = parameters['xml_sidecar_file_mapping']

        #
        # is there a xml sidecar file?
        #

        xmlfilename = self.get_xml_filename(filename, mapping)

        if verbose:

            if xmlfilename:

                print('XML sidecar file: {}'.format(xmlfilename))

            else:
                print("No xml sidecar file")

        #
        # read meta data from the XML sidecar file
        #
        if xmlfilename:

            if verbose:
                print("Reading XML sidecar file ".format(xmlfilename))
            try:

                # Parse the XML file
                parser = ElementTree.XMLParser()
                et = ElementTree.parse(xmlfilename, parser)
                root = et.getroot()

                for child in root:
                    self.elements2data(element=child, path=root.tag, data=data)

            except BaseException as e:
                sys.stderr.write(
                    "Exception while parsing XML {} {}".format(xmlfilename, e))

        return parameters, data
