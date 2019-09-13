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

    from argparse import ArgumentParser

    # get uri or filename and (optional) parameters from args

    def key_val(s):
        return s.split("=")

    parser = ArgumentParser("etl-file")
    parser.add_argument("-q", "--quiet",
                        action="store_true",
                        default=None,
                        help="Don\'t print status (filenames) while indexing")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true",
                        default=None, help="Print debug messages")
    parser.add_argument("-f", "--force", dest="force", action="store_true",
                        default=None,
                        help="Force (re)indexing, even if no changes")
    parser.add_argument("-c", "--config",
                        help="Config file")
    parser.add_argument("-p", "--plugins",
                        type=lambda s: s.split(","),
                        help="Plugin chain to use instead configured "
                        "plugins (comma separated and in order)")
    parser.add_argument("-a", "--additional-plugins",
                        dest="additional_plugins",
                        type=lambda s: s.split(","),
                        help="Plugins to add to default/configured plugins"
                        " (comma separated and in order)")
    parser.add_argument("-w", "--outputfile", dest="outputfile",
                        help="Output file")
    parser.add_argument("--param", action="append",
                        type=key_val,
                        help="Set a config parameter (key=value). "
                        "Can be specified multiple times")
    parser.add_argument("args", nargs="+", help="Input files")

    options = {key: val for key, val in vars(parser.parse_args()).items()
               if val is not None}

    args = options.pop("args")

    connector = Connector_File()

    # add optional config parameters
    config = options.pop("config", None)
    if config:
        connector.read_configfile(config)

    plugins = options.pop("plugins", []) + \
        options.pop("additional_plugins", [])

    # set (or if config overwrite) plugin config
    if plugins:
        connector.config['plugins'] = plugins

    connector.config.update(dict(options.pop("param", {})))

    connector.config.update(options)

    # index each filename
    for filename in args:
        connector.index(filename)

    # commit changes, if not yet done automatically by index timer
    connector.commit()
