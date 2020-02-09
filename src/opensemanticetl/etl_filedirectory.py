#!/usr/bin/python3
# -*- coding: utf-8 -*-

from etl_file import Connector_File

#
# Parallel processing of files by adding each file to celery tasks
#


class Connector_Filedirectory(Connector_File):

    def __init__(self, verbose=False, quiet=False):

        Connector_File.__init__(self, verbose=verbose)

        self.quiet = quiet

        # apply filters before adding to queue, so filtered or yet indexed files not added to queue
        # adding to queue by plugin export_queue_files

        # exporter to index filenames before text extraction and other later tasks
        # will run before adding tasks to queue by export_queue_files
        # so reseted plugin status will be in index before started ETL tasks apply not modified filter
        export_to_index = self.config['export']

        self.config['plugins'] = [
            'enhance_mapping_id',
            'filter_blacklist',
            'filter_file_not_modified',
            'enhance_file_mtime',
            'enhance_path',
            'enhance_entity_linking',
            'enhance_multilingual',
            export_to_index,
            'export_queue_files',
        ]

#
# Read command line arguments and start
#


# if running (not imported to use its functions), run main function
if __name__ == "__main__":

    from optparse import OptionParser

    # get uri or filename from args

    parser = OptionParser("etl-filedirectory [options] filename")
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true",
                      default=None, help="Don\'t print status (filenames) while indexing")
    parser.add_option("-v", "--verbose", dest="verbose",
                      action="store_true", default=None, help="Print debug messages")

    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("No filename given")

    connector = Connector_Filedirectory()

    if options.verbose == False or options.verbose == True:
        connector.verbose = options.verbose

    if options.quiet == False or options.quiet == True:
        connector.quiet = options.quiet

    # index each filename
    for filename in args:
        connector.index(filename)
