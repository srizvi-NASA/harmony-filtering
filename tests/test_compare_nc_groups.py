"""Unit tests for the copy_group function in filtering_utility.core module."""

# -*- coding: utf-8 -*-

# pylint: disable=missing-docstring, import-error, redefined-builtin, global-variable-not-assigned

import unittest
from unittest.mock import MagicMock

import numpy as np  # type: ignore

# Patch the compare_nc_groups to use above log_msg
from harmony_filtering_service.compare import compare_nc_groups
from harmony_filtering_service.logger import Logger

# pylint: disable=global-statement


# Assuming compare_nc_groups and log_msg are imported from the module under test.
# For this test code, we redefine log_msg to capture the logs for assertions:

logged_messages = []


class LocalLogger(Logger):
    def log(self, message: str) -> None:
        logged_messages.append(message)


class TestCompareNCGroups(unittest.TestCase):
    """Unit tests for the compare_nc_groups function."""

    def setUp(self):
        global logged_messages

        # Reset the logged messages before each test
        logged_messages = []

    def make_dim(self, length):
        """Create a mock NetCDF dimension with a specified length."""

        dim = MagicMock()
        dim_len = length
        dim.__len__.side_effect = lambda: dim_len
        return dim

    def make_var(self, dims=(), attrs=None, data=None):
        """Create a mock NetCDF variable with specified dimensions, attributes, and data."""
        var = MagicMock()
        var.dimensions = dims
        attrs = attrs or {}
        var.ncattrs.return_value = list(attrs.keys())
        var.getncattr.side_effect = lambda attr: attrs[attr]
        data = data if data is not None else np.array([])
        var.__getitem__.side_effect = lambda s: data if s == slice(None) else None
        return var

    def make_group(self, dims=None, attrs=None, vars=None, groups=None):
        """Create a mock NetCDF group with specified dimensions, attributes, variables, and subgroups."""

        group = MagicMock()
        dims = dims or {}
        attrs = attrs or {}
        vars = vars or {}
        groups = groups or {}
        group.dimensions = dims
        group.ncattrs.return_value = list(attrs.keys())
        group.getncattr.side_effect = lambda attr: attrs[attr]
        group.variables = vars
        group.groups = groups
        return group

    def test_missing_dimension_and_size_mismatch(self):
        """Test that missing dimensions and size mismatches are logged correctly."""

        src_dims = {"time": self.make_dim(10), "lat": self.make_dim(5)}
        filt_dims = {
            "time": self.make_dim(9)  # size mismatch
            # 'lat' missing
        }
        src_grp = self.make_group(dims=src_dims)
        filt_grp = self.make_group(dims=filt_dims)

        logger = LocalLogger(
            log_level="DEBUG",
            log_to_console=False,
            log_to_file=False,
            log_file_path="",
            granule_name="",
        )

        compare_nc_groups(src_grp, filt_grp, logger=logger)

        self.assertIn(
            "Dimension 'lat' missing in filtered file in group ''.", logged_messages
        )
        self.assertIn(
            "Dimension 'time' size mismatch in group '': original 10 vs filtered 9.",
            logged_messages,
        )

    def test_group_attributes_missing_and_mismatch(self):
        """Test that missing group attributes and mismatched attributes are logged correctly."""

        src_attrs = {
            "attr1": 5,
            "attr2": np.array([1, 2, 3]),
            "attr3": "value",
        }
        filt_attrs = {
            "attr1": 5,
            # missing attr2
            "attr3": "different",
        }
        src_grp = self.make_group(attrs=src_attrs)
        filt_grp = self.make_group(attrs=filt_attrs)

        logger = LocalLogger(
            log_level="DEBUG",
            log_to_console=False,
            log_to_file=False,
            log_file_path="",
            granule_name="",
        )

        compare_nc_groups(src_grp, filt_grp, logger=logger)

        self.assertIn(
            "Group attribute 'attr2' missing in filtered file in group ''.",
            logged_messages,
        )
        self.assertIn("Group attribute 'attr3' mismatch in group ''.", logged_messages)

    def test_variable_missing_and_dim_mismatch_and_attr_and_data(self):
        """Test that missing variables, dimension mismatch, attribute mismatch, and data differences are logged correctly."""

        var1 = self.make_var(
            dims=("time",), attrs={"units": "m/s"}, data=np.array([1, 2, 3])
        )
        var2_src = self.make_var(
            dims=("time", "lat"), attrs={}, data=np.array([[1, 2], [3, 4]])
        )
        var2_filt = self.make_var(
            dims=("time",), attrs={}, data=np.array([[1, 2], [3, 4]])
        )
        var3_src = self.make_var(
            dims=("time",),
            attrs={"standard_name": "temperature", "long_name": "temp"},
            data=np.array([10.0, np.nan, 30.0]),
        )
        var3_filt = self.make_var(
            dims=("time",),
            attrs={"standard_name": "temperature_mismatch"},  # long_name missing
            data=np.array([10.1, np.nan, 29.9]),
        )
        var4_src = self.make_var(
            dims=("time",), attrs={"_FillValue": -9999}, data=np.array([0.0, 1.0, 2.0])
        )
        var4_filt = self.make_var(
            dims=("time",), attrs={"_FillValue": -9999}, data=np.array([0.0, 1.0, 2.0])
        )

        src_vars = {"var1": var1, "var2": var2_src, "var3": var3_src, "var4": var4_src}
        filt_vars = {"var2": var2_filt, "var3": var3_filt, "var4": var4_filt}

        src_grp = self.make_group(vars=src_vars)
        filt_grp = self.make_group(vars=filt_vars)

        logger = LocalLogger(
            log_level="DEBUG",
            log_to_console=False,
            log_to_file=False,
            log_file_path="",
            granule_name="",
        )

        compare_nc_groups(src_grp, filt_grp, logger=logger)

        self.assertIn(
            "Variable 'var1' missing in filtered file in group ''.", logged_messages
        )
        self.assertIn(
            "Variable 'var2' dimensions mismatch in group '': original ('time', 'lat') vs filtered ('time',).",
            logged_messages,
        )
        self.assertIn(
            "Variable 'var3' attribute 'long_name' missing in filtered file in group ''.",
            logged_messages,
        )
        self.assertIn(
            "Variable 'var3' attribute 'standard_name' mismatch in group ''.",
            logged_messages,
        )
        self.assertIn(
            "Data differences detected in variable 'var3' in group '' for non-NaN values.",
            logged_messages,
        )
        self.assertIn(
            "Variable 'var4' in group '': data match for non-NaN values.",
            logged_messages,
        )

    def test_subgroup_missing_and_recursive_call(self):
        """Test that missing subgroups are logged correctly and recursion works as expected."""

        sub1 = self.make_group()
        src_grp = self.make_group(groups={"sub1": sub1})
        filt_grp = self.make_group(groups={})

        logger = LocalLogger(
            log_level="DEBUG",
            log_to_console=False,
            log_to_file=False,
            log_file_path="",
            granule_name="",
        )

        compare_nc_groups(src_grp, filt_grp, current_group="root", logger=logger)

        self.assertIn(
            "Subgroup 'sub1' missing in filtered file under group 'root'.",
            logged_messages,
        )

        # Now add sub1 to filt_grp and verify recursion proceeds silently
        filt_grp.groups["sub1"] = self.make_group()
        logged_messages.clear()
        compare_nc_groups(src_grp, filt_grp, current_group="root", logger=None)
        self.assertEqual(logged_messages, [])


if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)
