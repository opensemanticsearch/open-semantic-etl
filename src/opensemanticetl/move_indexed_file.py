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


def change_path(prefix):
    """Returns a mapping function to be used with starmap
    """
    def change(data: dict, dest: str) -> dict:
        """Creates a modified version of data

        :data: The indexed metadata of the moved file
        :dest: The destination path
        """
        _, *path_components, base_name = dest.split("/")
        moved_data = data.copy()
        del moved_data["_version_"]
        moved_data["id"] = prefix + dest
        moved_data.update({"path{}_s".format(i): component
                           for i, component in enumerate(path_components)})
        moved_data["path_basename_s"] = base_name
        n = len(path_components)
        while True:
            if moved_data.pop("path{}_s".format(n), None) is None:
                break
            n += 1
        return moved_data
    return change


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
    response = urllib.request.urlopen(
        host + "select?q="
        + urllib.parse.quote("(" + ", ".join(
            map('id:"{}"'.format, ids)) + ")"))
    data = json.loads(response.read().decode())
    return data["response"]["docs"]


def post(host: str, data: dict):
    request = urllib.request.Request(
        host + "update/json?commit=true",
        data=json.dumps(data).encode(),
        headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(request)
