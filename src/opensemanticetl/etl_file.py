#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os.path
import sys

from etl import ETL


class Connector_File(ETL):

    def __init__(self, verbose=False, quiet=True):

        ETL.__init__(self, verbose=verbose)

        self.quiet = quiet

        self.set_configdefaults()

        self.read_configfiles()

    def set_configdefaults(self):
        #
        # Standard config
        #
        # Do not edit config here!
        # Overwrite options in /etc/etl/
        # or /etc/opensemanticsearch/connector-files
        #

        ETL.set_configdefaults(self)

        self.config['force'] = False

        # filename to URI mapping
        self.config['mappings'] = {"/": "file:///"}

        self.config['facet_path_strip_prefix'] = [
            "file://",
            "http://www.", "https://www.", "http://", "https://"]

        self.config['plugins'] = [
            'enhance_mapping_id',
            'filter_blacklist',
            'filter_file_not_modified',
            'enhance_extract_text_tika_server',
            'enhance_detect_language_tika_server',
            'enhance_contenttype_group',
            'enhance_pst',
            'enhance_csv',
            'enhance_file_mtime',
            'enhance_path',
            'enhance_extract_hashtags',
            'enhance_warc',
            'enhance_zip',
            'clean_title',
            'enhance_multilingual',
        ]

        self.config['blacklist'] = [
            "/etc/opensemanticsearch/blacklist/blacklist-url"]
        self.config['blacklist_prefix'] = [
            "/etc/opensemanticsearch/blacklist/blacklist-url-prefix"]
        self.config['blacklist_suffix'] = [
            "/etc/opensemanticsearch/blacklist/blacklist-url-suffix"]
        self.config['blacklist_regex'] = [
            "/etc/opensemanticsearch/blacklist/blacklist-url-regex"]
        self.config['whitelist'] = [
            "/etc/opensemanticsearch/blacklist/whitelist-url"]
        self.config['whitelist_prefix'] = [
            "/etc/opensemanticsearch/blacklist/whitelist-url-prefix"]
        self.config['whitelist_suffix'] = [
            "/etc/opensemanticsearch/blacklist/whitelist-url-suffix"]
        self.config['whitelist_regex'] = [
            "/etc/opensemanticsearch/blacklist/whitelist-url-regex"]

    def read_configfiles(self):
        #
        # include configs
        #

        # Windows style filenames
        self.read_configfile('conf\\opensemanticsearch-etl')
        self.read_configfile('conf\\opensemanticsearch-enhancer-rdf')
        self.read_configfile('conf\\opensemanticsearch-connector-files')

        # Linux style filenames
        self.read_configfile('/etc/etl/config')
        self.read_configfile('/etc/opensemanticsearch/etl')
        self.read_configfile('/etc/opensemanticsearch/etl-webadmin')
        self.read_configfile('/etc/opensemanticsearch/etl-custom')
        self.read_configfile('/etc/opensemanticsearch/enhancer-rdf')
        self.read_configfile('/etc/opensemanticsearch/connector-files')
        self.read_configfile('/etc/opensemanticsearch/facets')
        self.read_configfile('/etc/opensemanticsearch/connector-files-custom')

    # clean filename (convert filename given as URI to filesystem)
    def clean_filename(self, filename):

        # if exist delete prefix file://

        if filename.startswith("file://"):
            filename = filename.replace("file://", '', 1)

        return filename

    # index directory or file
    def index(self, filename):

        # clean filename (convert filename given as URI to filesystem)
        filename = self.clean_filename(filename)

        # if singe file start to index it
        if os.path.isfile(filename):

            self.index_file(filename=filename)

            result = True

        # if directory walkthrough
        elif os.path.isdir(filename):

            self.index_dir(rootDir=filename)

            result = True

        # else error message
        else:

            result = False

            sys.stderr.write(
                "No such file or directory: {}\n".format(filename))

        return result

    # walk trough all sub directories and call index_file for each file
    def index_dir(self, rootDir, followlinks=False):

        for dirName, subdirList, fileList in os.walk(rootDir,
                                                     followlinks=followlinks):

            if self.verbose:
                print("Scanning directory: {}".format(dirName))

            for fileName in fileList:
                if self.verbose:
                    print("Scanning file: {}".format(fileName))

                try:

                    fullname = dirName
                    if not fullname.endswith(os.path.sep):
                        fullname += os.path.sep
                    fullname += fileName

                    self.index_file(filename=fullname)

                except KeyboardInterrupt:
                    raise KeyboardInterrupt
                except BaseException as e:
                    try:
                        sys.stderr.write(
                            "Exception while processing file {}{}{} : {}\n"
                            .format(dirName, os.path.sep, fileName, e))
                    except BaseException:
                        sys.stderr.write(
                            "Exception while processing a file and exception "
                            "while printing error message (maybe problem with"
                            " encoding of filename on console or converting "
                            "the exception to string?)\n")

    # Index a file
    def index_file(self, filename, additional_plugins=()):

        # clean filename (convert filename given as URI to filesystem)
        filename = self.clean_filename(filename)

        # fresh parameters / chain for each file (so processing one file will
        # not change config/parameters for next, if directory or multiple
        # files, which would happen if given by reference)
        parameters = self.config.copy()
        if additional_plugins:
            parameters['plugins'].extend(additional_plugins)

        if self.verbose:
            parameters['verbose'] = True

        data = {}

        if not 'id' in parameters:
            parameters['id'] = filename

        parameters['filename'] = filename

        parameters, data = self.process(parameters=parameters, data=data)

        return parameters, data

#
# Read command line arguments and start
#


# if running (not imported to use its functions), run main function
if __name__ == "__main__":

    from optparse import OptionParser

    # get uri or filename and (optional) parameters from args

    parser = OptionParser("etl-file [options] filename")
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true",
                      default=None, help="Don\'t print status (filenames) while indexing")
    parser.add_option("-v", "--verbose", dest="verbose",
                      action="store_true", default=None, help="Print debug messages")
    parser.add_option("-f", "--force", dest="force", action="store_true",
                      default=None, help="Force (re)indexing, even if no changes")
    parser.add_option("-c", "--config", dest="config",
                      default=False, help="Config file")
    parser.add_option("-p", "--plugins", dest="plugins", default=False,
                      help="Plugin chain to use instead configured plugins (comma separated and in order)")
    parser.add_option("-a", "--additional-plugins", dest="additional_plugins", default=False,
                      help="Plugins to add to default/configured plugins (comma separated and in order)")
    parser.add_option("-w", "--outputfile", dest="outputfile",
                      default=False, help="Output file")

    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("No filename given")

    connector = Connector_File()

    # add optional config parameters
    if options.config:
        connector.read_configfile(options.config)

    if options.outputfile:
        connector.config['outputfile'] = options.outputfile

    # set (or if config overwrite) plugin config
    if options.plugins:
        connector.config['plugins'] = options.plugins.split(',')

    # add addional plugin
    if options.additional_plugins:
        connector.config['plugins'].extend(
            options.additional_plugins.split(','))

    if options.verbose == False or options.verbose == True:
        connector.verbose = options.verbose

    if options.quiet == False or options.quiet == True:
        connector.quiet = options.quiet

    if options.force == False or options.force == True:
        connector.config['force'] = options.force

    # index each filename
    for filename in args:
        connector.index(filename)

    # commit changes, if not yet done automatically by index timer
    connector.commit()
