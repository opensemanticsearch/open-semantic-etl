#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import urllib.request
import xml.etree.ElementTree as ElementTree
from dateutil import parser as dateparser
import re

from etl_web import Connector_Web
import tasks


class Connector_Sitemap(Connector_Web):

    def __init__(self, verbose=False, quiet=True):

        Connector_Web.__init__(self, verbose=verbose, quiet=quiet)

        self.quiet = quiet
        self.read_configfiles()
        self.queue = True

    def read_configfiles(self):
        #
        # include configs
        #

        # windows style filenames
        self.read_configfile('conf\\opensemanticsearch-connector')
        self.read_configfile('conf\\opensemanticsearch-enhancer-ocr')
        self.read_configfile('conf\\opensemanticsearch-enhancer-rdf')
        self.read_configfile('conf\\opensemanticsearch-connector-web')

        # linux style filenames
        self.read_configfile('/etc/opensemanticsearch/etl')
        self.read_configfile('/etc/opensemanticsearch/etl-webadmin')
        self.read_configfile('/etc/opensemanticsearch/etl-custom')
        self.read_configfile('/etc/opensemanticsearch/enhancer-ocr')
        self.read_configfile('/etc/opensemanticsearch/enhancer-rdf')
        self.read_configfile('/etc/opensemanticsearch/connector-web')

    # Import sitemap

    # Index every URL of the sitemap

    def index(self, sitemap):

        if self.verbose or self.quiet == False:
            print("Downloading sitemap {}".format(sitemap))

        sitemap = urllib.request.urlopen(sitemap)

        et = ElementTree.parse(sitemap)

        root = et.getroot()

        # process subsitemaps if sitemapindex
        for sitemap in root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap"):
            url = sitemap.findtext(
                '{http://www.sitemaps.org/schemas/sitemap/0.9}loc')

            if self.verbose or self.quiet == False:
                print("Processing subsitemap {}".format(url))

            self.index(url)

        #
        # get urls if urlset
        #

        urls = []

        # XML schema with namespace sitemaps.org
        for url in root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}url"):

            url = url.findtext(
                '{http://www.sitemaps.org/schemas/sitemap/0.9}loc')

            urls.append(url)

        # XML schema with namespace Google sitemaps
        for url in root.findall("{http://www.google.com/schemas/sitemap/0.84}url"):

            url = url.findtext(
                '{http://www.google.com/schemas/sitemap/0.84}loc')

            urls.append(url)

        # Queue or download and index the urls

        for url in urls:

            if self.queue:

                # add webpage to queue as Celery task
                try:

                    if self.verbose or self.quiet == False:
                        print("Adding URL to queue: {}".format(url))

                    result = tasks.index_web.apply_async(
                        kwargs={'uri': url}, queue='tasks', priority=5)

                except KeyboardInterrupt:
                    raise KeyboardInterrupt
                except BaseException as e:
                    sys.stderr.write(
                        "Exception while adding to queue {} : {}\n".format(url, e))

            else:

                # batchmode, index page after page ourselves

                try:
                    if self.verbose or self.quiet == False:
                        print("Indexing {}".format(url))

                    result = Connector_Web.index(self, uri=url)

                except KeyboardInterrupt:
                    raise KeyboardInterrupt
                except BaseException as e:
                    sys.stderr.write(
                        "Exception while indexing {} : {}\n".format(url, e))

#
# If runned (not imported for functions) get parameters and start
#


if __name__ == "__main__":

    # get uri or filename from args
    from optparse import OptionParser
    parser = OptionParser("etl-sitemap [options] uri")
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true",
                      default=False, help="Don't print status (filenames) while indexing")
    parser.add_option("-v", "--verbose", dest="verbose",
                      action="store_true", default=None, help="Print debug messages")
    parser.add_option("-b", "--batch", dest="batchmode", action="store_true",
                      default=None, help="Batch mode (Page after page instead of adding to queue)")
    parser.add_option("-c", "--config", dest="config",
                      default=False, help="Config file")
    parser.add_option("-p", "--plugins", dest="plugins",
                      default=False, help="Plugins (comma separated)")

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.error("No sitemap uri(s) given")

    connector = Connector_Sitemap()

    # add optional config parameters
    if options.config:
        connector.read_configfile(options.config)

    # set (or if config overwrite) plugin config
    if options.plugins:
        connector.config['plugins'] = options.plugins.split(',')

    if options.verbose == False or options.verbose == True:
        connector.verbose = options.verbose

    if options.quiet == False or options.quiet == True:
        connector.quiet = options.quiet

    if options.batchmode == True:
        connector.queue = False

    for uri in args:
        connector.index(uri)
