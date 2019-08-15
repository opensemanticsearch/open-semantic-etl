import unittest
from unittest import mock

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
