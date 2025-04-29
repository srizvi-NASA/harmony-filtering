import argparse
import shutil
import os
from tempfile import mkdtemp
from pystac import Asset

import harmony_service_lib
from harmony_service_lib.util import generate_output_filename, stage, download

# ensure ENV is set (you can also pass -e ENV=dev in Docker)
os.environ.setdefault('ENV', 'dev')

class FilteringAdapter(harmony_service_lib.BaseHarmonyAdapter):
    def process_item(self, item, source):
        result = item.clone()
        result.assets = {}

        workdir = mkdtemp()
        try:
            asset = next(v for k, v in item.assets.items() if 'data' in (v.roles or []))
            input_filename = download(
                asset.href,
                workdir,
                logger=self.logger,
                access_token=self.message.accessToken
            )

            # your business logic
            working_filename = os.path.join(workdir, 'tmp.txt')
            shutil.copyfile(input_filename, working_filename)

            output_filename = generate_output_filename(
                asset.href,
                ext=None,
                variable_subset=None,
                is_regridded=False,
                is_subsetted=False
            )

            raw_loc = getattr(self.message, 'stagingLocation', None)
            loc = raw_loc if (raw_loc and raw_loc.startswith('s3://')) else None

            url = stage(
                working_filename,
                output_filename,
                'text/plain',
                location=loc,
                logger=self.logger
            )

            # ── DEV mode: persist the real file for inspection ──
            if os.environ.get('ENV') == 'dev':
                dev_out = os.path.join('/tmp', output_filename)
                shutil.copyfile(working_filename, dev_out)
                self.logger.info(f"DEV→ copied real output to {dev_out}")

            result.assets['data'] = Asset(
                url,
                title=output_filename,
                media_type='text/plain',
                roles=['data']
            ) 

            return result
        finally:
            shutil.rmtree(workdir)

def main():
    parser = argparse.ArgumentParser(prog='filtering', description='Run the filtering service')
    harmony_service_lib.setup_cli(parser)
    args = parser.parse_args()

    if harmony_service_lib.is_harmony_cli(args):
        harmony_service_lib.run_cli(parser, args, FilteringAdapter)
    else:
        parser.error("Only --harmony CLIs are supported")

if __name__ == "__main__":
    main()
