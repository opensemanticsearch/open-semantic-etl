#!/usr/bin/env python3

import urllib.request
import urllib.parse
import json
from itertools import starmap


def move_files(host: str, moves: dict, prefix=""):
    """Moves files within the index (not physically).

    Example of usage:
    host = "http://solr:8983/solr/opensemanticsearch/"
    move_files(host, {"/b2": "/book2",
                      "/b1": "/folder/book1"}, prefix="file://")


    :host: Url to the solr instance
    :moves: A dict of the form {src: dest, ...}, where src is
            the source path and dest is the destination path.
    """
    src = moves.keys()
    indexed_data = get_files(host, map(append_prefix(prefix), src))
    # In the following we have to remap the destination path to
    # the individual metadata entries, since the ordering of
    # the query result and query may differ:
    moved_data = starmap(change_path(prefix),
                         zip(indexed_data,
                             map(dict_map(moves),
                                 map(extract_path,
                                     indexed_data))))
    request_payload = prepare_payload(
        moved_data, (d["id"] for d in indexed_data))
    post(host, request_payload)


def move_dir(host: str, src: str, dest: str, prefix=""):
    """Moves directories within the index (not physically).

    Example of usage:
    host = "http://solr:8983/solr/opensemanticsearch/"
    move_dir(host, src=/docs/a/, dest=/docs/b, prefix="file://")


    :host: Url to the solr instance
    :src: Source directory
    :dest: Destination directory
    """
    indexed_data = get_files_in_dir(host, src)
    moved_data = map(change_dir(prefix, src=src, dest=dest),
                     indexed_data)
    request_payload = prepare_payload(
        moved_data, (d["id"] for d in indexed_data))
    post(host, request_payload)


def change_path(prefix: str):
    """Returns a mapping function to be used with starmap
    """
    def change(data: dict, dest: str) -> dict:
        """Creates a modified version of data

        :data: The indexed metadata of the moved file
        :dest: The destination path
        """
        dest_components = dest.strip("/").split("/")
        return _change_path(data, dest_components, prefix=prefix)
    return change


def change_dir(prefix: str, src: str, dest: str):
    """Returns a mapping function to be used with map
    """
    dest_components = dest.strip("/").split("/")
    src_path_components = src.strip("/").split("/")

    def change(data: dict) -> dict:
        """Creates a modified version of data

        :data: The indexed metadata of the moved file
        :dest: The destination path
        """
        indexed_components = extract_path_components(data)
        # Attention: zip consumes the generator up to the number
        # of elements in indexed_components. If you switch the two
        # arguments of zip, an additional element will be consumed
        # from indexed_components, as zip will perform a next on
        # its first argument to see if the iterable is exhausted.
        for idx_component, src_component in zip(src_path_components,
                                                indexed_components):
            if idx_component != src_component:
                raise ValueError(
                    "Path component of file and input file differs: '"
                    + idx_component + "' <-> '" + src_component + "'")
        return _change_path(data, dest_components + list(indexed_components),
                            prefix=prefix)
    return change


def _change_path(data: dict, dest_components: tuple, prefix: str = "") -> dict:
    """Creates a modified version of data

    :data: The indexed metadata of the moved file
    :dest_components: The destination path split into components
    """
    moved_data = data.copy()
    del moved_data["_version_"]
    moved_data["id"] = prefix + "/" + "/".join(dest_components)
    *dest_dir_components, base_name = dest_components
    moved_data.update({"path{}_s".format(i): component
                       for i, component in enumerate(dest_dir_components)})
    moved_data["path_basename_s"] = base_name
    n = len(dest_dir_components)
    while True:
        if moved_data.pop("path{}_s".format(n), None) is None:
            break
        n += 1
    return moved_data


def prepare_payload(adds, delete_ids):
    """Takes metadata to be added to the index and ids to be deleted
    from the index. Creates the corresponding solr json request payload
    """
    payload = {DuplicateKey("add"): {"doc": doc} for doc in adds}
    payload["delete"] = [
        {"id": id_} for id_ in delete_ids]
    return payload


class DuplicateKey(str):
    """Allows dicts having multiple identical keys"""

    def __hash__(self):
        return id(self)


def extract_path(data: dict) -> str:
    """Extracts the path of the metadata
    """
    return "/" + "/".join(extract_path_components(data))


def extract_path_components(data: dict):
    """Extracts the path of the metadata in form of a
    generator yielding the components of the path
    """
    i = 0
    while True:
        component = data.get("path{}_s".format(i))
        if component is None:
            break
        yield component
        i += 1
    yield data["path_basename_s"]


def dict_map(mapping: dict):
    """Converts a dict into a function (for usage with map)"""
    def _map(s):
        return mapping[s]
    return _map


def append_prefix(prefix: str):
    """A mapping function to be used with :map:"""
    def append(s: str):
        return prefix + s
    return append


def get_files(host: str, ids: list) -> list:
    """Queries solr, searches for files whose id is in :ids:"""
    return get(host,
               "(" + ", ".join(
                   map('id:"{}"'.format, ids)) + ")"
               )


def get_files_in_dir(host: str, path: str) -> list:
    """Queries solr, searches for files in the folder :path:"""
    path_components = path.strip("/").split("/")
    return get(host,
               " AND ".join(
                   starmap(
                       'path{}_s:"{}"'.format, enumerate(path_components)
                   )))


def get(host: str, query: str) -> list:
    return sum(get_pages(host, query), [])


def get_pages(host: str, query: str, limit=50):
    """An iterator over the pages of a solr request response"""
    start = 0
    n_docs = limit
    query_url_template = host + "select?start={}&rows={}&q={}".format(
        "{}", limit, urllib.parse.quote(query))
    while start < n_docs:
        response = urllib.request.urlopen(
            query_url_template.format(start))
        data = json.loads(response.read().decode())["response"]
        n_docs = data["numFound"]
        start += limit
        yield data["docs"]


def post(host: str, data: dict):
    request = urllib.request.Request(
        host + "update/json?commit=true",
        data=json.dumps(data).encode(),
        headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(request)
