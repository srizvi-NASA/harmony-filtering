# src/harmony_filtering_service/adapter.py
import argparse
import json
import os
import sys
import shutil
import xarray as xr
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Optional
from urllib.parse import urlparse, unquote

import harmony_service_lib
from harmony_service_lib.util import download, stage
from pystac import Asset, Item

from harmony_filtering_service.adapter_utils import \
    load_and_prepare_settings  # see below
from harmony_filtering_service.core import process_products

# ensure ENV=dev if nothing else
#os.environ.setdefault("ENV", "dev")

# In adapter.py, update your flatten_product_group to also drop the unwanted vars:

import xarray as xr
from pathlib import Path

def flatten_product_group(src: Path) -> Path:
    """
    Read src, pull all vars from the 'product' group up into the root,
    drop weight + the specified product‐group vars, write to a new
    '_flattened' file, and return that path.
    """
    ds0 = xr.open_dataset(src)

    # 1) merge product group if present
    try:
        with xr.open_dataset(src, group="product") as ds_prod:
            ds0.update(ds_prod)
    except OSError:
        # no 'product' group → nothing to flatten or drop in it
        pass

    # 2) drop unwanted variables if they exist
    vars_to_drop = [
        "weight",
        "vertical_column_troposphere",
        "vertical_column_troposphere_uncertainty",
        "main_data_quality_flag",
    ]
    present = [v for v in vars_to_drop if v in ds0]
    if present:
        ds0 = ds0.drop_vars(present)

    # 3) write out flattened+pruned file
    out = src.with_name(src.stem + "_flattened.nc")
    ds0.to_netcdf(out, mode="w")
    return out


def convert_time_and_stage(src: Path) -> str:
    """
    Read src with decode_times=False, apply decode_cf(use_cftime),
    write to /tmp/<basename>_cf.nc, and return the path to stage.
    """
    # 1) Open without decoding
    ds = xr.open_dataset(src, decode_times=False)
    # 2) Apply CF decoder with cftime
    ds_cf = xr.decode_cf(ds, use_cftime=True)

    # 3) Write out to a temp file under /tmp (owned by dockeruser)
    tmp = Path("/tmp") / (src.stem + "_cf.nc")
    ds_cf.to_netcdf(tmp, mode="w")

    return str(tmp)

class FilteringAdapter(harmony_service_lib.BaseHarmonyAdapter):  # type: ignore[misc]
    def process_item(self, item: Item, source: Any) -> Item:
        result = item.clone()
        result.assets = {}

        # 1) download into a throw-away dir
        workdir = mkdtemp()
        try:
            asset = next(v for v in item.assets.values() if "data" in (v.roles or []))

            local_in = download(
                asset.href,
                workdir,
                logger=self.logger,
                access_token=self.message.accessToken,
            )


            # 2) load and prepare settings.json (creates data_dir & output_dir)
            base = Path(__file__).parent.parent
            settings_path = base / "config" / "settings.json"
            settings = load_and_prepare_settings(settings_path)

            # 3) move the downloaded file into data_dir
            data_dir = Path(settings["data_dir"])
            data_dir.mkdir(parents=True, exist_ok=True)
            parsed = urlparse(asset.href)
            in_fname = Path(unquote(parsed.path)).name
            staged_input = data_dir / in_fname
            shutil.copy(local_in, staged_input)

            self.logger.info("Rashid-1 staged_input: %s", staged_input)

            # 4) load your filtering config.json
            cfg = json.loads(
                (base / "config" / "config.json").read_text(encoding="utf-8")
            )

            # 5) run your core filtering logic
            process_products(settings, cfg)

            # 6) find the one filtered file and stage it
            out_dir = Path(settings["output_dir"])
            base_stem = staged_input.stem
            filtered = out_dir / f"{base_stem}_filtered.nc"

            flat = Path(flatten_product_group(filtered))

            to_stage = convert_time_and_stage(flat)

            self.logger.info("Rashid-2 time changed to cftime: %s", to_stage)            

            # # Patch
            # raw_loc: Optional[str] = getattr(self.message, "stagingLocation", None)
            # loc: Optional[str] = (
            #      raw_loc if (raw_loc and raw_loc.startswith("s3://")) else None
            # )
            
            url = stage(
                to_stage,
                filtered.name,             # keep the original filename for downstream
                "application/x-netcdf",
                location=self.message.stagingLocation,
                logger=self.logger
            )
            # url = stage(
            #     str(filtered),
            #     filtered.name,
            #     "application/x-netcdf",
            #     location=self.message.stagingLocation,
            #     #location=loc,
            #     logger=self.logger
            # )

            # # ── DEV mode: persist the real file for inspection ──
            # if os.environ.get("ENV") == "dev":
            #      # copy from the real output_dir into /tmp
            #      src = str(
            #          filtered
            #      )  # full path, e.g. /worker/data/out_data/..._filtered.nc
            #      dev_out = os.path.join("/tmp", filtered.name)
            #      shutil.copyfile(src, dev_out)
            #      self.logger.info(f"DEV→ copied real output to {dev_out}")

            # ── Copy into your host-mounted folder ──
            host_output = Path("/host-output")
            host_output.mkdir(parents=True, exist_ok=True)
            dest = host_output / filtered.name
            #shutil.copyfile(str(filtered), str(dest))
            shutil.copyfile(str(to_stage), str(dest))
            self.logger.info(f"Copied filtered file to {dest}")

            # add it to the STAC
            result.assets["data"] = Asset(
                href=url,
                title=filtered.name,
                media_type="application/x-netcdf",
                roles=["data"],
            )

            return result

        finally:
            shutil.rmtree(workdir)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="filtering", description="Run the filtering service"
    )
    harmony_service_lib.setup_cli(parser)
    args = parser.parse_args()
    if harmony_service_lib.is_harmony_cli(args):
        harmony_service_lib.run_cli(parser, args, FilteringAdapter)
    else:
        parser.error("Only --harmony CLIs are supported")


if __name__ == "__main__":
    main()
