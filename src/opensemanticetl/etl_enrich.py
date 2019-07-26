#!/usr/bin/python3
# -*- coding: utf-8 -*-

from etl import ETL
import pysolr
import export_solr
import importlib
import threading

# Todo: Abstraction of querying data to a function of output plugin
# so this will work for other index or database than Solr, too

# Todo: Yet problem, if you run only enrichment plugins (i.e. OCR) without container plugins (i.e. mailbox extractor or ZIP archive extractor), if you want to run this plugin with a container plugin:
# because the container is marked as done in first run (but was not extract because container/extraction plugin not active) so next run with this continer_plugins/extractors wont enrich them anymore

# Since we use enrichment of queries only for OCR after indexing in Open Semantic Desktop Search where we know we call the OCR plugin with all container plugins, fixing or maybe making this perfect later (maybe better classification/management of container plugins or defining in plugins, if they need access to file)


class ETL_Enrich(ETL):

    def __init__(self, 	plugins=[], verbose=False):

        ETL.__init__(self, plugins=plugins, verbose=verbose)

        self.read_configfile('/etc/etl/config')
        self.read_configfile('/etc/opensemanticsearch/etl')
        self.read_configfile('/etc/opensemanticsearch/enhancer-rdf')

        self.fields = self.getfieldnames_from_plugins()

        # init exporter	(todo: exporter as extended PySolr)
        self.export_solr = export_solr.export_solr()

        # init PySolr
        solr_uri = self.config['solr']
        if not solr_uri.endswith('/'):
            solr_uri += '/'
        solr_uri += self.config['index']

        self.solr = pysolr.Solr(solr_uri)

        self.threads_max = None

        # if not set explicit, autodetection of count of CPUs for amount of threads
        if not self.threads_max:
            import multiprocessing
            self.threads_max = multiprocessing.cpu_count()
            if self.verbose:
                print("Setting threads to count of CPUs: " +
                      str(self.threads_max))

        self.rows_per_step = 100
        if self.rows_per_step < self.threads_max * 2:
            self.rows_per_step = self.threads_max * 2

        self.work_in_progress = []
        self.delete_from_work_in_progress_lock = threading.Lock()

        self.delete_from_work_in_progress_after_commit = []
        self.work_in_progress_lock = threading.Lock()

        self.e_job_done = threading.Event()

    #
    # get all the fields needed by all plugins for analysis
    #

    def getfieldnames_from_plugins(self):

        # the field id is needed for every enrichment
        fields = ['id']

        # read all fieldnames, the plugins need to analyze
        for plugin in self.config['plugins']:

            module = importlib.import_module(plugin)
            objectreference = getattr(module, plugin, False)
            if objectreference:
                modulefields = getattr(objectreference, 'fields', False)
            if modulefields:
                for field in modulefields:
                    # only if not there yet from other plugin
                    if not field in fields:
                        fields.append(field)

        return fields

    #
    # Start ETL process / run of all set plugins
    #

    def enrich_document(self, docid):

        try:

            if self.verbose:
                print("Enriching {}".format((docid)))

            parameters = self.config.copy()

            #
            # read data from analyzed fields and add to parameters
            #

            # id is only field, so we do not have to get it again from index or database
            if len(self.fields) == 1:
                parameters['id'] = docid

            # if more than id needed add fields from DB/index to parameters since that data is analysed by the plugins
            else:

                data = self.export_solr.get_data(
                    docid=docid, fields=self.fields)

                # add  to by analysed data of the first and only result to ETL/Enrichment parameters
                parameters.update(data)

            filename = docid
            # if exist delete protocol prefix file://
            if filename.startswith("file://"):
                filename = filename.replace("file://", '', 1)

            parameters['filename'] = filename

            parameters['verbose'] = self.verbose

            if self.verbose:
                print("Parameters:")
                print(parameters)

            # set markers that enriched by this plugins
            data = {}
            for plugin in self.config['plugins']:
                data['etl_' + plugin + '_b'] = True

            # start ETL / Enrichment process
            parameters, data = self.process(parameters=parameters, data=data)

        finally:

            # remove blacklisting/locking for this document, since enrichment process is now done
            self.work_in_progress_lock.acquire()

            if docid in self.work_in_progress:
                self.delete_from_work_in_progress_lock.acquire()

                self.delete_from_work_in_progress_after_commit.append(docid)

                self.delete_from_work_in_progress_lock.release()

            self.work_in_progress_lock.release()

            # set event, so main thread wakes up and knows next job/thread can be started
            self.e_job_done.set()

    #
    # get query from plugin and start enrichment process for this query
    #

    # not usable for plugin chains (i.e. extract containers like ZIP files and than OCR contents)! Use enrich_query with a compounded query instead.
    def enrich(self):

        for plugin in self.config['plugins']:

            query = "*:* AND NOT (etl_{}_b:true)".format(plugin)

            # check if plugin has own more special query and if so, use this
            module = importlib.import_module(plugin)
            objectreference = getattr(module, plugin, False)
            if objectreference:
                query = getattr(objectreference, 'query', query)

            if self.verbose:
                print("Data enrichment query: {}".format(query))

            # enrich
            self.enrich_query(query)

    # get ids from query
    # get fields from plugins
    # run enrichment chain with this parameters
    def enrich_query(self, query):

        counter = 0

        solrparameters = {
            'fl': 'id',
            'rows': self.rows_per_step,
        }

        # have to proceed all documents matching this query:

        # - all yet not enriched (query for content type AND not plugin_b: true) results of enriched content type (plugin query)
        # OR
        # - container file type like ZIP archive or PST mailbox
        # - AND not yet enriched by all set plugins
        #
        # - but both cases not if no file but content of a container file (for subfiles the enrichment plugins will be runned by the run of the container plugin)

        # query matching i.e. the contenttype of to be enriched files (for example if doing OCR we do only have to process images, not all documents)
        running_plugin_query = '(' + query + ')'

        # not, if all set plugins yet runned on this document
        all_plugin_query = []
        for plugin in self.config['plugins']:
            all_plugin_query.append("(etl_{}_b:true)".format(plugin))
        all_plugin_query = '(' + ' AND '.join(all_plugin_query) + ')'

        # matching container content types like archive files
        # since our content types like f.e. images can be in containers like for example ZIP archives, so we should enrich them too)
        container_query = '(content_type:application\/zip OR id:*.zip OR content_type:application\/vnd.ms-outlook-pst OR id:*.pst)'
        # Todo for more performance:
        # distinct container_s from results with query for only needed content types instead of working trough all containers

        query = running_plugin_query + \
            ' OR (' + container_query + ' AND NOT ' + all_plugin_query + ')'

        # not try to enrich (not existent in filesystem) subfiles inside container files like ZIP archives or extracted mail attachments
        # since for subfiles the enrichment plugins will be runned by the run of the container plugin)
        query = '(' + query + ') AND NOT (container_s:*)'

        if self.verbose:
            print("Enrichment of matches the following query:")
            print(query)

        results = self.solr.search(query, **solrparameters)

        while len(results) > 0:

            for result in results:

                docid = result['id']

                if self.threads_max == 1:

                    # no threading, do it directly in this process
                    self.enrich_document(docid=docid)
                    counter += 1

                else:

                    #
                    # Manage threading
                    #

                    # If doc id blacklistet (work in progress in running threads) don't start thread for docid,
                    # since new search result can include some same documents again which not yet ready enriched because work goes on in a thread from step before.
                    # So continue with next result.
                    if docid in self.work_in_progress:
                        continue

                    # wait for a job done if yet maximum threads (+1 because do not count this main thread) running
                    while threading.active_count() >= self.threads_max + 1:
                        # wait for event that signals that a thread/job finished (set in enrich_document() at end)
                        # use a timeout if racing condition (setting in finished job before clearing this event here)
                        self.e_job_done.wait(1)

                    # blacklist id of document work in progres
                    self.work_in_progress_lock.acquire()
                    self.work_in_progress.append(docid)
                    self.work_in_progress_lock.release()

                    # start enrichment of this document in new thread
                    thread = threading.Thread(
                        target=self.enrich_document, args=(docid, ))
                    self.e_job_done.clear()
                    thread.start()

                    counter += 1

            # do commit, so next query wont find documents again, which were processed but not available for searcher yet
            self.commit()

            #
            # delete done IDs from blacklist
            #

            self.delete_from_work_in_progress_lock.acquire()

            while len(self.delete_from_work_in_progress_after_commit) > 0:
                docid = self.delete_from_work_in_progress_after_commit.pop()

                self.work_in_progress_lock.acquire()
                self.work_in_progress.remove(docid)
                self.work_in_progress_lock.release()

            self.delete_from_work_in_progress_lock.release()

            #
            # If last step (fewer search results than a step manages), wait for all threads to be done
            # before starting new search / next step (which will only find the documents again, that are work in progress in running threads and not done/marked as ready yet) to prevent unnecessary search load
            #

            if len(results) < self.rows_per_step:
                # wait until all started threads done before continue (commit and end),

                while threading.active_count() > 1:
                    # wait for event that signals that a thread/job finished (set in enrich_document() at end)
                    # use a timeout if racing condition (setting in finished job before clearing this event here)
                    self.e_job_done.wait(1)
                    self.e_job_done.clear()

                # commit last results, so very last enrichments have not wait the next autocommit time (if set up)
                self.commit()

            # are there (more) not yet enriched documents in search index for a next step?
            results = self.solr.search(query, **solrparameters)

        # if self.verbose:
        print("Enriched {} documents".format(counter))


# todo: export to Solr by update

if __name__ == "__main__":

    # get uri or filename from args
    from optparse import OptionParser

    parser = OptionParser("etl-enrich [options] --plugins pluginname")

    parser.add_option("-p", "--plugins", dest="plugins",
                      default=False, help="Plugins (comma separated)")
    parser.add_option("-c", "--config", dest="config",
                      default=False, help="Config file")
    parser.add_option("-q", "--query", dest="query",
                      default=False, help="Query")
    parser.add_option("-o", "--outputfile", dest="outputfile", default=False,
                      help="Output file (if exporter set to a file format)")
    parser.add_option("-v", "--verbose", dest="verbose",
                      action="store_true", default=None, help="Print debug messages")
    (options, args) = parser.parse_args()

    etl = ETL_Enrich()

    if options.config:
        etl.read_configfile(options.config)
        etl.fields = etl.getfieldnames_from_plugins()

    if options.verbose == False or options.verbose == True:
        etl.verbose = options.verbose

    # set (or if config overwrite) plugin config
    if options.plugins:
        etl.config['plugins'] = options.plugins.split(',')
        etl.fields = etl.getfieldnames_from_plugins()

    if options.outputfile:
        etl.config['outputfile'] = options.outputfile

    # if query, enrich IDs matching this query
    if options.query:

        etl.enrich_query(options.query)

    # if no query and no id's as argument, use default query from plugins
    elif len(args) == 0:

        etl.enrich()

    # if not query but IDs
    else:

        for uri in args:

            # todo: if not local file, download to temp file if a plugin needs parameter filename

            etl.enrich_document(docid=uri)

        etl.commit()
