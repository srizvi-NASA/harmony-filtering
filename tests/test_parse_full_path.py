"""Unit tests for the copy_group function in filtering_utility.core module."""

# -*- coding: utf-8 -*-

# pylint: disable=import-error, missing-docstring, invalid-name, line-too-long, protected-access

import unittest

from harmony_filtering_service.core import parse_full_path
from harmony_filtering_service.exceptions import FilteringUtilityError


class TestParseFullPath(unittest.TestCase):
    def test_parse_valid_full_path(self):
        group, variable = parse_full_path("group1/varA")
        self.assertEqual(group, "group1")
        self.assertEqual(variable, "varA")

    def test_parse_full_path_with_multiple_slashes_only_returns_first_two_parts(self):
        group, variable = parse_full_path("group1/subgroup/varA")
        # According to code, only first two parts returned
        self.assertEqual(group, "group1")
        self.assertEqual(variable, "subgroup")

    def test_parse_invalid_no_slash_raises(self):
        with self.assertRaises(FilteringUtilityError) as context:
            parse_full_path("invalidpath")
        self.assertIn(
            "not in the expected 'group/variable' format", str(context.exception)
        )


if __name__ == "__main__":
    unittest.main()
