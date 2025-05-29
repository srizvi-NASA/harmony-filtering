import json
from pathlib import Path
from typing import Any, Dict, cast


def load_and_prepare_settings(path: Path) -> Dict[str, Any]:
    """
    Read settings.json, resolve `data_dir` and `output_dir` to absolute Paths,
    create those directories, and return the settings as a dict.
    """
    # Load raw JSON (returns Any), cast to expected dict type
    raw = json.loads(path.read_text(encoding="utf-8"))
    settings: Dict[str, Any] = cast(Dict[str, Any], raw)

    # Ensure data_dir and output_dir exist, update to absolute strings
    for key in ("data_dir", "output_dir"):
        p = Path(settings[key]).expanduser().resolve()
        p.mkdir(parents=True, exist_ok=True)
        settings[key] = str(p)

    return settings
