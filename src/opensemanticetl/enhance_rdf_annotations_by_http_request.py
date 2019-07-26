import sys
import hashlib
import urllib
import rdflib
from rdflib import URIRef

# Do templating of metaserver url for id


def metaserver_url(metaserver, docid):

    metaurl = metaserver

    metaurl = metaurl.replace('[uri]', urllib.parse.quote_plus(docid))

    h = hashlib.md5(docid.encode("utf-8"))
    metaurl = metaurl.replace(
        '[uri_md5]', urllib.parse.quote_plus(h.hexdigest()))

    return metaurl


# get the modification date of meta data
# todo: check all metaservers, not only the last one and return latest date

def getmeta_modified(metaservers, docid, verbose=False):

    if isinstance(metaservers, str):
        metaserver = metaservers
    else:
        for server in metaservers:
            metaserver = server

    metaurl = metaserver_url(metaserver, docid)

    moddate = False

    if verbose:
        print("Getting Meta from {}".format(metaurl))

    try:
        g = rdflib.Graph()
        result = g.parse(metaurl)

        # if semantic mediawiki modification date field, take this as date

        for subj, pred, obj in g.triples((None, URIRef("http://semantic-mediawiki.org/swivt/1.0#wikiPageModificationDate"), None)):

            # todo only if later than previos, if more than one (f.e. more than one metaserver)
            moddate = str(obj)

        if verbose:
            print("Extracted modification date: {}".format(moddate))

        if verbose:
            if not moddate:
                print("No modification date for metadata")

    except BaseException as e:
        sys.stderr.write(
            "Exception while getting metadata modification time: {}\n".format(e.args[0]))

    return moddate


# Get tagging and annotation from metadata server
def getmeta_rdf_from_server(metaserver, data, property2facet, docid, verbose=False):

    moddate = False

    metaurl = metaserver_url(metaserver, docid)

    if verbose:
        print("Getting Meta from {}".format(metaurl))

    g = rdflib.Graph()
    result = g.parse(metaurl)

    # Print infos
    if verbose:
        print("Meta graph has {} statements.".format(len(g)))
        for subj, pred, obj in g:

            try:
                print("{} : {}".format(pred, obj.toPython))
            except BaseException as e:
                sys.stderr.write(
                    "Exception while printing triple: {}\n".format(e.args[0]))

    # make solr iteral for each rdf tripple contained in configurated properties
    for facet in property2facet:

        # if this predicat is configured as facet, add literal with pred as facetname and object as value
        try:
            if verbose:
                print('Checking Facet {}'.format(facet))

            facetRef = URIRef(facet)

            for subj, pred, obj in g.triples((None, facetRef, None)):
                try:

                    # add the facet with object as value
                    solr_facet = property2facet[facet]

                    if verbose:
                        print("Adding Solr facet {} with the object {}".format(
                            solr_facet, obj))

                    if solr_facet in data:
                        data[solr_facet].append(obj.toPython())
                    else:
                        data[solr_facet] = [obj.toPython()]

                except BaseException as e:
                    sys.stderr.write(
                        "Exception while checking predicate {}{}\n".format(pred, e.args[0]))

        except BaseException as e:
            sys.stderr.write(
                "Exception while checking a part of metadata graph: {}\n".format(e.args[0]))

    # if semantic mediawiki modification date field, take this as date
    moddateRef = URIRef(
        "http://semantic-mediawiki.org/swivt/1.0#wikiPageModificationDate")
    if (None, moddateRef, None) in g:
        for subj, pred, obj in g.triples((None, moddateRef, None)):
            moddate = obj.toPython()

        # todo: transform date format to date and in exporter date to Solr date string format
        #data['meta_modified_dt'] = str(moddate)

        if verbose:
            print("Extracted modification date: {}".format(moddate))

    elif verbose:
        print("No semantic mediawiki modification date")

    return data


# Get tagging and annotation from metadata server

class enhance_rdf_annotations_by_http_request(object):

    def process(self, parameters={}, data={}):

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        # get parameters
        docid = parameters['id']

        metaserver = parameters['metaserver']
        property2facet = parameters['property2facet']

        # if only one server
        if isinstance(metaserver, str):
            # get metadata
            data = getmeta_rdf_from_server(
                metaserver=metaserver, data=data, property2facet=property2facet, docid=docid, verbose=verbose)
        else:

            # list of servers
            for server in metaserver:
                # get and add metadata
                data = getmeta_rdf_from_server(
                    metaserver=server, data=data, property2facet=property2facet, docid=docid, verbose=verbose)

        return parameters, data
