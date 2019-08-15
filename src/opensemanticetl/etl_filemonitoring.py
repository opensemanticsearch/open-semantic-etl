#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pyinotify

from optparse import OptionParser

from tasks import index_file
from tasks import delete

from etl import ETL
from enhance_mapping_id import mapping

from move_indexed_file import move_files


class EventHandler(pyinotify.ProcessEvent):

    def __init__(self):
        super().__init__()
        self.verbose = False
        self.config = {}

    def process_IN_CLOSE_WRITE(self, event):
        if self.verbose:
            print("Close_write: {}".format(event.pathname))

        self.index_file(filename=event.pathname)

    def process_IN_MOVED_TO(self, event):
        if self.verbose:
            print("Move: {} -> {}".format(event.src_pathname, event.pathname))

        self.move_file(src=event.src_pathname, dest=event.pathname)

    def process_IN_DELETE(self, event):

        if self.verbose:
            print("Delete {}:".format(event.pathname))

        self.delete_file(filename=event.pathname)

    #
    # write to queue
    #

    def move_file(self, src, dest):
        if self.verbose:
            print("Moving file from {} to {}".format(src, dest))
        solr_uri = self.config["solr"] + self.config["index"]
        if not solr_uri.endswith("/"):
            solr_uri += "/"
        move_files(solr_uri, moves={src: dest}, prefix="file://")

    def index_file(self, filename):
        if self.verbose:
            print("Indexing file {}".format(filename))

        index_file.apply_async(
            kwargs={'filename': filename}, queue='tasks', priority=5)

    def delete_file(self, filename):
        uri = filename
        if 'mappings' in self.config:
            uri = mapping(value=uri, mappings=self.config['mappings'])

        if self.verbose:
            print("Deleting from index filename {} with URL {}".format(
                filename, uri))

        delete.apply_async(kwargs={'uri': uri}, queue='tasks', priority=6)


class Filemonitor(ETL):

    def __init__(self, verbose=False):

        ETL.__init__(self, verbose=verbose)

        self.verbose = verbose

        self.read_configfiles()

        self.mask = pyinotify.IN_DELETE | pyinotify.IN_CLOSE_WRITE | pyinotify.IN_MOVED_TO | pyinotify.IN_MOVED_FROM  # watched events

        self.watchmanager = pyinotify.WatchManager()  # Watch Manager

        self.handler = EventHandler()

        self.notifier = pyinotify.Notifier(self.watchmanager, self.handler)

    def read_configfiles(self):
        #
        # include configs
        #

        self.read_configfile('/etc/opensemanticsearch/etl')
        self.read_configfile('/etc/opensemanticsearch/connector-files')

    def add_watch(self, filename):

        self.watchmanager.add_watch(
            filename, self.mask, rec=True, auto_add=True)

    @staticmethod
    def add_watches_from_file(filename):
        listfile = open(filename)
        for line in listfile:
            filename = line.strip()
            # ignore empty lines and comment lines (starting with #)
            if filename and not filename.startswith("#"):

                filemonitor.add_watch(filename)

    def watch(self):

        self.handler.config = self.config
        self.handler.verbose = self.verbose

        self.notifier.loop()


# parse command line options
parser = OptionParser("etl-filemonitor [options] filename")
parser.add_option("-v", "--verbose", dest="verbose",
                  action="store_true", default=False, help="Print debug messages")
parser.add_option("-f", "--fromfile", dest="fromfile",
                  default=False, help="File names config")
(options, args) = parser.parse_args()


filemonitor = Filemonitor(verbose=options.verbose)


# add watches for every file/dir given as command line parameter
for _filename in args:
    filemonitor.add_watch(_filename)


# add watches for every file/dir in list file
if options.fromfile:
    filemonitor.add_watches_from_file(options.fromfile)


# start watching
filemonitor.watch()
