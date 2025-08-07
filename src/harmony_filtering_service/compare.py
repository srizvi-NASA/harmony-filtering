"""
Comparison module for Filtering Utility.

This module provides functions to compare the structure and data of 2 netCDF files.
"""

from typing import Any

import numpy as np
from netCDF4 import Dataset as ncDataset

from harmony_filtering_service.logger import log_msg


def compare_nc_groups(
    src_grp: Any, filt_grp: Any, current_group: str = "", logger: Any = None
) -> None:
    """
    Recursively compare groups of two netCDF files.

    Parameters:
        src_grp: Source netCDF group.
        filt_grp: Filtered netCDF group.
        current_group: Current group path used for logging.
        logger: Logger instance to log messages.
    """
    # Compare dimensions
    for dim_name, dim in src_grp.dimensions.items():
        # Disable membership test warning for filt_grp.dimensions
        if dim_name not in filt_grp.dimensions:  # pylint: disable=unsupported-membership-test
            log_msg(
                f"Dimension '{dim_name}' missing in filtered file in group '{current_group}'.",
                logger,
            )
        else:
            # Disable unsubscriptable warning for filt_grp.dimensions
            if len(dim) != len(filt_grp.dimensions[dim_name]):  # pylint: disable=unsubscriptable-object
                log_msg(
                    f"Dimension '{dim_name}' size mismatch in group '{current_group}': original {len(dim)} vs filtered {len(filt_grp.dimensions[dim_name])}.",
                    logger,
                )
    # Compare group attributes
    for attr in src_grp.ncattrs():
        src_attr = src_grp.getncattr(attr)
        if attr not in filt_grp.ncattrs():
            log_msg(
                f"Group attribute '{attr}' missing in filtered file in group '{current_group}'.",
                logger,
            )
        else:
            filt_attr = filt_grp.getncattr(attr)
            if isinstance(src_attr, (list, np.ndarray, tuple)) or isinstance(
                filt_attr, (list, np.ndarray, tuple)
            ):
                if not np.array_equal(np.asarray(src_attr), np.asarray(filt_attr)):
                    log_msg(
                        f"Group attribute '{attr}' mismatch in group '{current_group}'.",
                        logger,
                    )
            else:
                if src_attr != filt_attr:
                    log_msg(
                        f"Group attribute '{attr}' mismatch in group '{current_group}'.",
                        logger,
                    )
    # Compare variables
    for var_name, var in src_grp.variables.items():
        if var_name not in filt_grp.variables:
            log_msg(
                f"Variable '{var_name}' missing in filtered file in group '{current_group}'.",
                logger,
            )
            continue
        filt_var = filt_grp.variables[var_name]
        if var.dimensions != filt_var.dimensions:
            log_msg(
                f"Variable '{var_name}' dimensions mismatch in group '{current_group}': original {var.dimensions} vs filtered {filt_var.dimensions}.",
                logger,
            )
        for attr in var.ncattrs():
            if attr == "_FillValue":
                continue
            src_attr = var.getncattr(attr)
            if attr not in filt_var.ncattrs():
                log_msg(
                    f"Variable '{var_name}' attribute '{attr}' missing in filtered file in group '{current_group}'.",
                    logger,
                )
            else:
                filt_attr = filt_var.getncattr(attr)
                if isinstance(src_attr, (list, np.ndarray, tuple)) or isinstance(
                    filt_attr, (list, np.ndarray, tuple)
                ):
                    if not np.array_equal(np.asarray(src_attr), np.asarray(filt_attr)):
                        log_msg(
                            f"Variable '{var_name}' attribute '{attr}' mismatch in group '{current_group}'.",
                            logger,
                        )
                else:
                    if src_attr != filt_attr:
                        log_msg(
                            f"Variable '{var_name}' attribute '{attr}' mismatch in group '{current_group}'.",
                            logger,
                        )
        src_data = var[:]
        filt_data = filt_var[:]
        # valid_mask = ~np.isnan(filt_data)
        # new — coerce to float and then make your mask
        filt_arr = np.asarray(filt_data, dtype=float)
        nan_mask = np.isnan(filt_arr)
        valid_mask = np.logical_not(nan_mask)
        if not np.allclose(src_data[valid_mask], filt_data[valid_mask], atol=1e-8):
            log_msg(
                f"Data differences detected in variable '{var_name}' in group '{current_group}' for non-NaN values.",
                logger,
            )
        else:
            log_msg(
                f"Variable '{var_name}' in group '{current_group}': data match for non-NaN values.",
                logger,
            )
    # Recursively compare subgroups
    for subgrp_name, subgrp in src_grp.groups.items():
        if subgrp_name not in filt_grp.groups:
            log_msg(
                f"Subgroup '{subgrp_name}' missing in filtered file under group '{current_group}'.",
                logger,
            )
        else:
            new_group = (
                f"{current_group}/{subgrp_name}" if current_group else subgrp_name
            )
            compare_nc_groups(subgrp, filt_grp.groups[subgrp_name], new_group, logger)


def compare_nc_files(
    original_file: str, filtered_file: str, logger: Any = None
) -> None:
    """
    Compare the structure and data of two netCDF files.

    Parameters:
        original_file: Path to the original netCDF file.
        filtered_file: Path to the filtered netCDF file.
        logger: Logger instance to log messages.
    """
    log_msg(
        f"\nComparing original file:\n  {original_file}\nwith filtered file:\n  {filtered_file}\n",
        logger,
    )
    src_nc = ncDataset(original_file, "r")
    filt_nc = ncDataset(filtered_file, "r")

    # Convert dimensions to a regular dictionary for safe subscripting
    filt_dims = dict(filt_nc.dimensions)

    for attr in src_nc.ncattrs():
        src_attr = src_nc.getncattr(attr)
        if attr not in filt_nc.ncattrs():
            log_msg(f"Global attribute '{attr}' missing in filtered file.", logger)
        else:
            filt_attr = filt_nc.getncattr(attr)
            if isinstance(src_attr, (list, np.ndarray, tuple)) or isinstance(
                filt_attr, (list, np.ndarray, tuple)
            ):
                if not np.array_equal(np.asarray(src_attr), np.asarray(filt_attr)):
                    log_msg(f"Global attribute '{attr}' mismatch.", logger)
            else:
                if src_attr != filt_attr:
                    log_msg(
                        f"Global attribute '{attr}' mismatch: original {src_attr} vs filtered {filt_attr}.",
                        logger,
                    )

    for dim_name, dim in src_nc.dimensions.items():  # pylint: disable=no-member
        if dim_name not in filt_dims:
            log_msg(f"Root dimension '{dim_name}' missing in filtered file.", logger)
        elif len(dim) != len(filt_dims[dim_name]):
            log_msg(
                f"Root dimension '{dim_name}' size mismatch: original {len(dim)} vs filtered {len(filt_dims[dim_name])}.",
                logger,
            )

    compare_nc_groups(src_nc, filt_nc, current_group="", logger=logger)

    src_nc.close()
    filt_nc.close()
    log_msg("Comparison complete.\n", logger)
