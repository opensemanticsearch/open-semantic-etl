#!/usr/bin/python3
# -*- coding: utf-8 -*-

import time
import urllib.request
import os
from lxml import etree
from dateutil import parser as dateparser

from etl_file import Connector_File


class Connector_Web(Connector_File):

    def __init__(self, verbose=False, quiet=True):

        Connector_File.__init__(self, verbose=verbose)

        self.quiet = quiet
        self.set_configdefaults()
        self.read_configfiles()

    def set_configdefaults(self):

        Connector_File.set_configdefaults(self)

        #
        # Standard config
        #
        # Do not edit config here! Overwrite options in /etc/opensemanticsearch/connector-web
        #

        # no filename to uri mapping
        self.config['uri_prefix_strip'] = False
        self.config['uri_prefix'] = False

        # strip in facet path
        self.config['facet_path_strip_prefix'] = ['http://www.',
                                                  'http://',
                                                  'https://www.',
                                                  'https://',
                                                  'ftp://'
                                                  ]

        self.config['plugins'] = [
            'filter_blacklist',
            'enhance_extract_text_tika_server',
            'enhance_detect_language_tika_server',
            'enhance_contenttype_group',
            'enhance_pst',
            'enhance_csv',
            'enhance_path',
            'enhance_zip',
            'enhance_warc',
            'enhance_extract_hashtags',
            'clean_title',
            'enhance_multilingual',

        ]

    def read_configfiles(self):
        #
        # include configs
        #

        # Windows style filenames
        self.read_configfile('conf\\opensemanticsearch-etl')
        self.read_configfile('conf\\opensemanticsearch-enhancer-rdf')
        self.read_configfile('conf\\opensemanticsearch-connector-web')

        # Linux style filenames
        self.read_configfile('/etc/opensemanticsearch/etl')
        self.read_configfile('/etc/opensemanticsearch/etl-webadmin')
        self.read_configfile('/etc/opensemanticsearch/etl-custom')
        self.read_configfile('/etc/opensemanticsearch/enhancer-rdf')
        self.read_configfile('/etc/opensemanticsearch/connector-web')
        self.read_configfile('/etc/opensemanticsearch/connector-web-custom')

    def read_mtime_from_html(self, tempfilename):
        mtime = False

        try:
            parser = etree.HTMLParser()
            tree = etree.parse(tempfilename, parser)

            try:
                mtimestring = tree.xpath(
                    "//meta[@http-equiv='last-modified']")[0].get("content")
            except:
                mtimestring = False

            try:
                mtimestring = tree.xpath(
                    "//meta[@name='last-modified']")[0].get("content")
            except:
                mtimestring = False
        except:
            mtimestring = False

        if mtimestring:

            if self.verbose:
                print("Modification time in HTML: ", mtimestring)

            try:
                mtime = time.strptime(mtimestring)
            except:
                mtime = False

            try:
                # parse datetime
                mtime = dateparser.parse(mtimestring)
                # convert datetime to time
                mtime = mtime.timetuple()

            except BaseException as e:
                print("Exception while reading last-modified from content: {}".format(e))

        if self.verbose:
            print("Extracted modification time: {}".format(mtime))

        return mtime

    def index(self, uri, last_modified=False, downloaded_file=False, downloaded_headers=None):
        if downloaded_headers is None:
            downloaded_headers = {}

        parameters = self.config.copy()

        if self.verbose:
            parameters['verbose'] = True

        data = {}

        uri = uri.strip()
        # if no protocol, add http://
        if not uri.lower().startswith("http://") and not uri.lower().startswith("https://") and not uri.lower().startswith("ftp://") and not uri.lower().startswith("ftps://"):
            uri = 'http://' + uri

        parameters['id'] = uri

        #
        # Download to tempfile, if not yet downloaded by crawler
        #

        if downloaded_file:
            tempfilename = downloaded_file
            headers = downloaded_headers

        else:

            if self.verbose:
                print("Downloading {}".format(uri))

            tempfilename, headers = urllib.request.urlretrieve(uri)

            if self.verbose:
                print("Download done")

        parameters['filename'] = tempfilename

        #
        # Modification time
        #
        mtime = False

        # get meta "last-modified" from content
        mtime = self.read_mtime_from_html(tempfilename)

        # use HTTP status modification time
        if not mtime:
            try:

                last_modified = headers['last-modified']

                if self.verbose:
                    print("HTTP Header Last-modified: {}".format(last_modified))

                mtime = dateparser.parse(last_modified)
                # convert datetime to time
                mtime = mtime.timetuple()

                if self.verbose:
                    print("Parsed date: {}".format(mtime))

            except:
                mtime = False
                print("Failed to parse HTTP header last-modified")

        # else HTTP create date
        if not mtime:
            try:
                date = headers['date']

                if self.verbose:
                    print("HTTP Header date: {}".format(date))

                mtime = dateparser.parse(date)
                # convert datetime to time
                mtime = mtime.timetuple()

                if self.verbose:
                    print("Parsed date: {}".format(mtime))

            except:
                mtime = False
                print("Failed to parse HTTP header date")

        # else now
        if not mtime:
            mtime = time.localtime()

        mtime_masked = time.strftime("%Y-%m-%dT%H:%M:%SZ", mtime)

        data['file_modified_dt'] = mtime_masked

        # Enrich data and write to search index
        parameters, data = self.process(parameters=parameters, data=data)

        os.remove(tempfilename)


#
# If runned (not importet for functions) get parameters and start
#

if __name__ == "__main__":

    # get uri or filename from args

    from optparse import OptionParser

    # get uri or filename from args

    parser = OptionParser("etl-web [options] URL")
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true",
                      default=None, help="Do not print status (filenames) while indexing")
    parser.add_option("-v", "--verbose", dest="verbose",
                      action="store_true", default=None, help="Print debug messages")
    parser.add_option("-f", "--force", dest="force", action="store_true",
                      default=None, help="Force (re)indexing, even if no changes")
    parser.add_option("-c", "--config", dest="config",
                      default=False, help="Config file")
    parser.add_option("-p", "--plugins", dest="plugins",
                      default=False, help="Plugins (comma separated)")
    parser.add_option("-w", "--outputfile", dest="outputfile",
                      default=False, help="Output file")

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.error("No URI(s) given")

    connector = Connector_Web()

    # add optional config parameters
    if options.config:
        connector.read_configfile(options.config)
    if options.outputfile:
        connector.config['outputfile'] = options.outputfile

    # set (or if config overwrite) plugin config
    if options.plugins:
        connector.config['plugins'] = options.plugins.split(',')

    if options.verbose == False or options.verbose == True:
        connector.verbose = options.verbose

    if options.quiet == False or options.quiet == True:
        connector.quiet = options.quiet

    if options.force == False or options.force == True:
        connector.config['force'] = options.force

    for uri in args:
        connector.index(uri)
