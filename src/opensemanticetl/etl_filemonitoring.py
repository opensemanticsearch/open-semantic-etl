#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pyinotify

from optparse import OptionParser

from tasks import index_file
from tasks import delete

from etl import ETL
from enhance_mapping_id import mapping


class EventHandler(pyinotify.ProcessEvent):

    def __init__(self):

        self.verbose = False
        self.config = {}

    def process_IN_CLOSE_WRITE(self, event):
        if self.verbose:
            print("Close_write: {}".format(event.pathname))

        self.process(filename=event.pathname, function="index-file")

    def process_IN_MOVED_TO(self, event):
        if self.verbose:
            print("Moved_to: {}".format(event.pathname))

        self.process(filename=event.pathname, function="index-file")

    def process_IN_MOVED_FROM(self, event):
        if self.verbose:
            print("Moved_from: {}".format(event.pathname))

        self.process(filename=event.pathname, function="delete")

    def process_IN_DELETE(self, event):

        if self.verbose:
            print("Delete {}:".format(event.pathname))

        self.process(filename=event.pathname, function="delete")

    #
    # write to queue
    #

    def process(self, filename, function):

        if function == 'index-file':

            if self.verbose:
                print("Indexing file {}".format(filename))

            index_file.apply_async(
                kwargs={'filename': filename}, queue='tasks', priority=5)

        elif function == 'delete':

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

    def add_watches_from_file(self, filename):
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
for filename in args:
    filemonitor.add_watch(filename)


# add watches for every file/dir in list file
if options.fromfile:
    filemonitor.add_watches_from_file(options.fromfile)


# start watching
filemonitor.watch()
