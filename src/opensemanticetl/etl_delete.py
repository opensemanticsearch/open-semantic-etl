#!/usr/bin/python3
# -*- coding: utf-8 -*-

import importlib

from etl import ETL


class Delete(ETL):
    def __init__(self, verbose=False, quiet=True):

        ETL.__init__(self, verbose=verbose)

        self.quiet = quiet

        self.set_configdefaults()

        self.read_configfiles()

        # read on what DB or search server software our index is
        export = self.config['export']

        # call delete function of the configured exporter
        module = importlib.import_module(export)
        objectreference = getattr(module, export)
        self.connector = objectreference()

    def set_configdefaults(self):
        #
        # Standard config
        #
        # Do not edit config here! Overwrite options in /etc/etl/ or /etc/opensemanticsearch/connector-files
        #

        ETL.set_configdefaults(self)

        self.config['force'] = False

    def read_configfiles(self):
        #
        # include configs
        #

        # Windows style filenames
        self.read_configfile('conf\\opensemanticsearch-etl')

        # Linux style filenames
        self.read_configfile('/etc/opensemanticsearch/etl')

    def delete(self, uri):

        if self.verbose:
            print("Deleting from index {}".format(uri))

        self.connector.delete(parameters=self.config, docid=uri)

    def empty(self):

        if self.verbose:
            print("Deleting all documents from index")

        self.connector.delete(parameters=self.config, query="*:*")


#
# Read command line arguments and start
#

# if running (not imported to use its functions), run main function
if __name__ == "__main__":

    from optparse import OptionParser

    # get uri or filename from args

    parser = OptionParser("etl-delete [options] URI(s)")
    parser.add_option("-e", "--empty", dest="empty", action="store_true",
                      default=False,
                      help="Empty the index (delete all documents in index)")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                      default=None, help="Print debug messages")
    parser.add_option("-c", "--config", dest="config", default=False,
                      help="Config file")

    (options, args) = parser.parse_args()

    if not options.empty and len(args) < 1:
        parser.error("No URI given")

    connector = Delete()

    connector.read_configfile('/etc/etl/config')

    # add optional config parameters
    if options.config:
        connector.read_configfile(options.config)

    if options.verbose == False or options.verbose == True:
        connector.verbose = options.verbose

    if options.empty:
        print(
            "This will delete the whole index, are you sure ? Then enter \"yes\"")
        descision = input()
        if descision == "yes":
            print("huhu")
            connector.empty()

    # index each filename
    for uri in args:
        connector.delete(uri)
