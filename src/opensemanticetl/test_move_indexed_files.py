import unittest
from unittest import mock

import json
import itertools

import move_indexed_file


class TestMove(unittest.TestCase):
    def test_move_files(self):
        mock_get_files = mock.Mock(return_value=[
            {'id': 'file:///book1', 'title_t': 'Snow Crash',
             'copies_i': 5, 'cat_ss': ['Science Fiction'],
             'path_basename_s': 'book1', '_version_': 1641756143516647424},
            {'id': 'file:///folder/book2',
             'title_t': 'Other book',
             'copies_i': 3, 'cat_ss': ['Round House Kicks'],
             'path0_s': 'folder',
             'path_basename_s': 'book2', '_version_': 1641756143518744576}])
        mock_post = mock.Mock()

        def mock_prepare(adds, delete_ids):
            self.assertEqual(
                list(adds),
                [
                    {'id': 'file:///snow_crash', 'title_t': 'Snow Crash',
                     'copies_i': 5, 'cat_ss': ['Science Fiction'],
                     'path_basename_s': 'snow_crash'},
                    {'id': 'file:///other_book',
                     'title_t': 'Other book',
                     'copies_i': 3, 'cat_ss': ['Round House Kicks'],
                     'path_basename_s': 'other_book'}])
            self.assertEqual(tuple(delete_ids),
                             ("file:///book1", "file:///folder/book2"))
        with mock.patch("move_indexed_file.get_files", mock_get_files), \
                mock.patch("move_indexed_file.post", mock_post), \
                mock.patch("move_indexed_file.prepare_payload",
                           mock_prepare):
            move_indexed_file.move_files(
                None, {"/book1": "/snow_crash",
                       "/folder/book2": "/other_book"},
                prefix="file://")

    def test_move_dir(self):
        mock_get_files = mock.Mock(return_value=[
            {'id': 'file:///folder/book1', 'title_t': 'Snow Crash',
             'copies_i': 5, 'cat_ss': ['Science Fiction'],
             'path0_s': 'folder',
             'path_basename_s': 'book1', '_version_': 1641756143516647424},
            {'id': 'file:///folder/book2',
             'title_t': 'Other book',
             'copies_i': 3, 'cat_ss': ['Round House Kicks'],
             'path0_s': 'folder',
             'path_basename_s': 'book2', '_version_': 1641756143518744576}])
        mock_post = mock.Mock()

        def mock_prepare(adds, delete_ids):
            self.assertEqual(
                list(adds),
                [
                    {'id': 'file:///dest/book1', 'title_t': 'Snow Crash',
                     'copies_i': 5, 'cat_ss': ['Science Fiction'],
                     'path0_s': 'dest',
                     'path_basename_s': 'book1'},
                    {'id': 'file:///dest/book2',
                     'title_t': 'Other book',
                     'copies_i': 3, 'cat_ss': ['Round House Kicks'],
                     'path0_s': 'dest',
                     'path_basename_s': 'book2'}])
            self.assertEqual(tuple(delete_ids),
                             ("file:///folder/book1", "file:///folder/book2"))
        with mock.patch("move_indexed_file.get_files_in_dir",
                        mock_get_files), \
                mock.patch("move_indexed_file.post", mock_post), \
                mock.patch("move_indexed_file.prepare_payload",
                           mock_prepare):
            move_indexed_file.move_dir(
                None, src="/folder", dest="/dest/",
                prefix="file://")

    def test_get_pages(self):
        step = 3

        original_docs = [{'id': i} for i in range(10)]

        def mock_urlopen():
            docs_iter = iter(original_docs)
            responses = (
                mock_response(
                    {
                        "response": {
                            "numFound": len(original_docs),
                            "docs": list(itertools.islice(docs_iter, step))
                        }
                    }
                )
                for __ in range(0, len(original_docs), step))

            def _mock(*_, **__):
                return next(responses)
            return _mock
        with mock.patch("urllib.request.urlopen",
                        mock_urlopen()):
            docs = sum(move_indexed_file.get_pages("", "", limit=step), [])
        self.assertEqual(docs, original_docs)


def mock_response(data):
    return type("MockResponse", (object,), {
        "read": json.dumps(data).encode})
