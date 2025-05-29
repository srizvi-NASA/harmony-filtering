import json
import os

import earthaccess


def get_download_dir() -> str:
    """
    Load the download directory from the settings.json file.

    The settings file is assumed to be located in the ../config/ directory relative to this script.
    The download directory is taken from the "test" section under the key "data_dir".

    Returns:
        The normalized path for downloading files.
    """
    # Construct the path to the settings.json file.
    settings_path = os.path.join(
        os.path.dirname(__file__), "..", "config", "settings.json"
    )
    # Open and load the JSON settings.
    with open(settings_path, "r", encoding="utf-8") as f:
        settings = json.load(f)
    # Retrieve the test data directory from the settings.
    # Default to "./tests/data/in_data" if not provided.
    download_dir = settings.get("test", {}).get("data_dir", "./tests/data/in_data")
    return os.path.normpath(download_dir)


def download_granule(concept_id: str) -> list:
    """
    Query and download granule(s) for a given concept ID.

    Parameters:
        concept_id: The concept id string used for querying the granule.

    Returns:
        A list of file paths downloaded.
    """
    # Query for the granule using earthaccess.
    granules = earthaccess.granule_query().concept_id(concept_id).get(1)
    # Get the download directory from settings.
    target_dir = get_download_dir()
    # Download the granules to the specified directory.
    files = earthaccess.download(granules, target_dir)
    return files


def main() -> None:
    """
    Main function that logs into earthaccess and downloads a granule for a hard-coded concept id.
    """
    concept_id = "C2930763263-LARC_CLOUD"  # TEMPO NO2 L3 V03
    # earthaccess.login(persist=True)
    earthaccess.login(strategy="environment")
    download_granule(concept_id)


if __name__ == "__main__":
    main()
