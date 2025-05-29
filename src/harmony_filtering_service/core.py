"""
Core module for Filtering Utility.

This module contains the core processing logic, including functions for
parsing filenames, copying netCDF groups, and processing products
based on provided settings and configuration.
"""

import glob
import os
import re
from typing import Any, Dict, Set

import numpy as np
import xarray as xr
from netCDF4 import Dataset as ncDataset

from harmony_filtering_service.compare import compare_nc_files
from harmony_filtering_service.exceptions import FilteringUtilityError
from harmony_filtering_service.logger import get_logger, log_msg


def parse_granule_filename(filename: str) -> Dict[str, str]:
    """
    Parse a TEMPO granule filename and extract metadata.

    The filename is expected to follow a format with parts separated by underscores, e.g.:
    "TEMPO_NO2_L3_V02_20240215T123255Z_S002.nc"
    where:
      - TEMPO is the instrument,
      - NO2 is the product,
      - L3 indicates the level (here we extract the digit "3"),
      - V02 is the version,
      - 20240215T123255Z is the timestamp, and
      - S002 is the sequence.

    Parameters:
        filename: TEMPO granule filename.

    Returns:
        A dictionary with keys: "instrument", "product", "level", "version", "timestamp", "sequence".
        Note: If the level cannot be determined, it defaults to an empty string.
    """
    parts = filename.split("_")
    instrument = parts[0]
    product = parts[1]
    level_str = parts[2]
    level_match = re.search(r"\d+", level_str)
    # Default to an empty string if the level is not found
    level = level_match.group(0) if level_match else ""
    version = parts[3]
    timestamp = parts[4]
    sequence = parts[5].split(".")[0]
    return {
        "instrument": instrument,
        "product": product,
        "level": level,
        "version": version,
        "timestamp": timestamp,
        "sequence": sequence,
    }


def parse_full_path(full_path: str) -> tuple[str, str]:
    """
    Parse a full path string in the format "group/variable" and return a tuple (group, variable).

    This function is used to determine in which group a variable is located
    inside the netCDF file.

    Parameters:
        full_path: A string in the format "group/variable".

    Returns:
        A tuple (group, variable_name).

    Raises:
        FilteringUtilityError: If the input does not contain a slash.
    """
    parts = full_path.split("/")
    if len(parts) < 2:
        raise FilteringUtilityError(
            f"Full path '{full_path}' is not in the expected 'group/variable' format."
        )
    return parts[0], parts[1]


def copy_group(
    src_grp: Any,
    dst_grp: Any,
    current_group: str,
    filtered_primary: Dict[str, Any],
    excluded_variables: Set[str],
    logger: Any = None,
) -> None:
    """
    Recursively copy a group from the source netCDF dataset to the destination.

    This function handles the duplication of dimensions, variables, and attributes.
    It applies special handling to variables: if a variable has a filtered version
    in 'filtered_primary', it uses that data; otherwise, it copies the original data.
    Variables whose full path (group/variable) is in the 'excluded_variables' set
    are skipped.

    Parameters:
        src_grp: The source netCDF group (from the original file).
        dst_grp: The destination netCDF group (for the filtered output).
        current_group: A string representing the current group path (empty string for root).
        filtered_primary: A dictionary mapping full variable paths to filtered arrays.
        excluded_variables: A set containing full paths of variables that should be excluded.
        logger: Optional logger instance for tracking progress and issues.
    """
    # Copy dimensions from the source group to the destination group
    for dim_name, dim in src_grp.dimensions.items():
        # For unlimited dimensions, pass None; otherwise, use the length of the dimension
        dst_grp.createDimension(dim_name, len(dim) if not dim.isunlimited() else None)

    # Iterate over all variables in the source group
    for var_name, var in src_grp.variables.items():
        # Construct the full variable path (including the group if applicable)
        full_var_path = f"{current_group}/{var_name}" if current_group else var_name

        # Skip this variable if it is in the exclusion list
        if full_var_path in excluded_variables:
            log_msg(
                f"Skipping excluded variable '{full_var_path}' in group '{current_group}'.",
                logger,
            )
            continue

        filters = var.filters() or {}
        zlib_flag = filters.get("zlib", False)
        complevel = filters.get("complevel", None)
        shuffle = filters.get("shuffle", False)
        # Check if a fill value is defined for the variable
        fill_value = (
            var.getncattr("_FillValue") if "_FillValue" in var.ncattrs() else None
        )

        # Create the variable in the destination file with similar settings
        if zlib_flag:
            if complevel is not None:
                dst_var = dst_grp.createVariable(
                    var_name,
                    var.datatype,
                    var.dimensions,
                    zlib=True,
                    complevel=complevel,
                    shuffle=shuffle,
                    fill_value=fill_value,
                )
            else:
                dst_var = dst_grp.createVariable(
                    var_name,
                    var.datatype,
                    var.dimensions,
                    zlib=True,
                    shuffle=shuffle,
                    fill_value=fill_value,
                )
        else:
            dst_var = dst_grp.createVariable(
                var_name, var.datatype, var.dimensions, fill_value=fill_value
            )

        # Copy variable attributes (skip _FillValue as it is already applied)
        for attr in var.ncattrs():
            if attr == "_FillValue":
                continue
            dst_var.setncattr(attr, var.getncattr(attr))

        # Determine if there's filtered data for this variable; if so, use it; otherwise, copy original data.
        key = f"{current_group}/{var_name}" if current_group else var_name
        if key in filtered_primary:
            dst_var[:] = filtered_primary[key].values
        else:
            dst_var[:] = var[:]

    # Copy global attributes from the source group
    for attr in src_grp.ncattrs():
        dst_grp.setncattr(attr, src_grp.getncattr(attr))

    # Recursively copy any subgroups within this group
    for subgrp_name, subgrp in src_grp.groups.items():
        new_subgrp = dst_grp.createGroup(subgrp_name)
        new_current_group = (
            f"{current_group}/{subgrp_name}" if current_group else subgrp_name
        )
        copy_group(
            subgrp,
            new_subgrp,
            new_current_group,
            filtered_primary,
            excluded_variables,
            logger,
        )


def process_products(settings: Dict[str, Any], config: Dict[str, Any]) -> None:
    """
    Main processing loop for filtering products.

    This function reads input data and configuration, applies filtering rules,
    saves the resulting filtered netCDF files, and then compares the original
    and filtered files for verification.

    Parameters:
        settings: Dictionary containing settings loaded from settings.json.
                  Expected keys include "data_dir", "output_dir", and "logging" settings.
        config: Dictionary containing product configuration loaded from config.json.
    """
    # Extract directories and logging settings from 'settings'
    data_dir = settings["data_dir"]
    output_dir = settings["output_dir"]
    log_to_console = settings["logging"]["log_to_console"]
    log_to_file = settings["logging"]["log_to_file"]
    log_file_path = settings["logging"]["log_file_path"]
    log_level = settings["logging"]["log_level"]

    # Ensure the output directory exists, create if not
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Build a set of variables to be excluded based on the configuration
    excluded_variables: Set[str] = set()
    for product_type in config:
        ev = (
            config[product_type]["filters"]
            .get("variable_exclusion", {})
            .get("excluded_variables", [])
        )
        excluded_variables.update(ev)

    # Process each product defined in the configuration
    for product_type in config:
        # Define the file pattern for netCDF files for this product
        pattern = os.path.join(data_dir, f"TEMPO_{product_type}_*.nc")
        file_list = glob.glob(pattern)

        # If no file is found, output a message to console (if enabled) and skip processing
        if not file_list:
            if log_to_console:
                print(f"No file found for product '{product_type}'. Skipping.")
            continue

        # Initialize a logger for this product only if files exist
        logger = get_logger(
            product_type, log_level, log_to_console, log_to_file, log_file_path
        )
        log_msg(f"Processing product '{product_type}'", logger)

        # Use the first file found for this product (could be extended to process all files)
        file_path = file_list[0]
        log_msg(f"File: {file_path}", logger)

        # Extract metadata from the filename for logging purposes
        filename = os.path.basename(file_path)
        metadata = parse_granule_filename(filename)
        log_msg("Metadata extracted from filename:", logger)
        log_msg(f"  Instrument: {metadata['instrument']}", logger)
        log_msg(f"  Product: {metadata['product']}", logger)
        log_msg(f"  Level: {metadata['level']}", logger)
        log_msg(f"  Version: {metadata['version']}", logger)
        log_msg(f"  Timestamp: {metadata['timestamp']}", logger)
        log_msg(f"  Sequence: {metadata['sequence']}", logger)

        granule_level = metadata["level"]
        # Obtain the filtering rules from the configuration
        product_filters = config[product_type]["filters"]["pixel_filter"]
        primary_full_paths = {rule["target_var"] for rule in product_filters}
        secondary_full_paths = {rule["criteria_var"] for rule in product_filters}

        # Identify the groups (or sub-structures) needed from the netCDF file
        groups_to_open = set()
        for fp in primary_full_paths.union(secondary_full_paths):
            grp, _ = parse_full_path(fp)
            groups_to_open.add(grp)

        # Open each required group using xarray for easier data handling
        opened_groups = {
            grp: xr.open_dataset(file_path, group=grp) for grp in groups_to_open
        }

        # Extract primary and secondary variables from each group
        primary_vars = {}
        for fp in primary_full_paths:
            grp, var_name = parse_full_path(fp)
            if var_name not in excluded_variables:
                primary_vars[fp] = opened_groups[grp][var_name]

        secondary_vars = {}
        for fp in secondary_full_paths:
            grp, var_name = parse_full_path(fp)
            secondary_vars[fp] = opened_groups[grp][var_name]

        # Log summary statistics (min, max, NaNs) for primary variables before filtering
        log_msg("Before applying filters:", logger)
        for fp, arr in primary_vars.items():
            min_val = float(arr.min().values)
            max_val = float(arr.max().values)
            total_nan = int(arr.isnull().sum())
            _, var_name = parse_full_path(fp)
            log_msg(
                f"  Primary variable '{var_name}': min = {min_val}, max = {max_val}, total NaNs = {total_nan}",
                logger,
            )

        any_filter_applied = False
        # Iterate over each filter rule in sorted order based on the rule key (converted to int for sorting)
        for idx, rule in enumerate(product_filters, start=1):
            rule_key = str(idx)  # Use the index as a string for logging if needed
            primary_full_path = rule["target_var"]
            secondary_full_path = rule["criteria_var"]
            filter_level = rule["level"]
            # Use a membership test to decide if the filter should be applied.
            # If the filter level is not "all" and does not match the granule level, skip it.
            if filter_level not in ("all", granule_level):
                log_msg(
                    f"Skipping filter rule '{rule_key}' due to level mismatch.",
                    logger,
                )
                continue
            # Skip the filter if the primary variable is excluded
            if primary_full_path in excluded_variables:
                log_msg(
                    f"Skipping filter rule '{rule_key}' as primary variable '{primary_full_path}' is excluded.",
                    logger,
                )
                continue

            operator = rule["operator"]
            threshold = float(rule["threshold"])
            target_value = (
                float(rule["target_value"]) if rule["target_value"] != "nan" else np.nan
            )

            # Apply the filtering logic if the primary variable is present.
            if primary_full_path in primary_vars:
                primary_array = primary_vars[primary_full_path]
                secondary_array = secondary_vars[secondary_full_path]
                # Construct a boolean mask based on the operator and threshold.
                if operator == "greater-than":
                    mask = secondary_array > threshold
                elif operator == "less-than":
                    mask = secondary_array < threshold
                elif operator == "greater-than-or-equal-to":
                    mask = secondary_array >= threshold
                elif operator == "less-than-or-equal-to":
                    mask = secondary_array <= threshold
                else:
                    raise FilteringUtilityError(
                        f"Unsupported operator '{operator}' in rule '{rule_key}'."
                    )
                sec_non_nan_count = int(primary_array.where(mask).notnull().sum())
                log_msg(
                    f"Filter rule '{rule_key}': secondary variable '{secondary_full_path}' non-nan count = {sec_non_nan_count}",
                    logger,
                )
                # If no pixels meet the criteria, skip this rule.
                if sec_non_nan_count == 0:
                    log_msg("No pixels to filter for this rule. Skipping.", logger)
                    continue
                # Apply filtering: if target_value is NaN, mark the pixel as filtered (NaN);
                # otherwise, clamp the pixel value to target_value.
                if np.isnan(target_value):
                    filtered_array = primary_array.where(~mask)
                else:
                    non_nan_mask = primary_array.notnull()
                    filtered_array = primary_array.where(
                        ~(mask & non_nan_mask), target_value
                    )
                primary_vars[primary_full_path] = filtered_array
                any_filter_applied = True

        if not any_filter_applied:
            log_msg("No filters applied. Copying original data.", logger)

        # Log primary variable statistics after filtering
        log_msg("After applying filters:", logger)
        for fp, arr in primary_vars.items():
            min_val = float(arr.min().values)
            max_val = float(arr.max().values)
            total_nan = int(arr.isnull().sum())
            _, var_name = parse_full_path(fp)
            log_msg(
                f"  Primary variable '{var_name}': min = {min_val}, max = {max_val}, total NaNs = {total_nan}",
                logger,
            )

        # Save the filtered output to a new netCDF file
        base_name = os.path.basename(os.path.splitext(file_path)[0])
        new_file_path = os.path.join(output_dir, base_name + "_filtered.nc")
        src_nc = ncDataset(file_path, "r")
        dst_nc = ncDataset(new_file_path, "w")
        # Copy global attributes from the original file
        for attr in src_nc.ncattrs():
            dst_nc.setncattr(attr, src_nc.getncattr(attr))
        # Copy the data (with any filtering applied) from the original file to the new file
        copy_group(src_nc, dst_nc, "", primary_vars, excluded_variables, logger)
        dst_nc.close()
        src_nc.close()
        log_msg(f"Filtered file saved as: {new_file_path}", logger)
        # Compare the original and filtered files for verification
        compare_nc_files(file_path, new_file_path, logger)
        # Close all opened groups to free resources
        for ds in opened_groups.values():
            ds.close()
        log_msg("=" * 60, logger)
        logger.close()
