"""Download JSONL files from tommycarstensen.com/epstein/."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

BASE_URL = "https://tommycarstensen.com/epstein"
DATASETS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
DATA_DIR = Path("data")


def download_jsonl(dataset: int, data_dir: Path = DATA_DIR) -> Path:
    """Download a single JSONL file."""
    data_dir.mkdir(parents=True, exist_ok=True)
    url = f"{BASE_URL}/set_{dataset}.jsonl"
    out_path = data_dir / f"set_{dataset}.jsonl"

    if out_path.exists():
        logger.info(f"Already downloaded: {out_path}")
        return out_path

    logger.info(f"Downloading {url}...")
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    with open(out_path, "wb") as f:
        with tqdm(total=total, unit="B", unit_scale=True, desc=f"set_{dataset}") as pbar:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))

    logger.info(f"Saved: {out_path}")
    return out_path


def download_all(data_dir: Path = DATA_DIR, datasets: list[int] | None = None) -> list[Path]:
    """Download all JSONL files."""
    datasets = datasets or DATASETS
    paths = []
    for ds in datasets:
        paths.append(download_jsonl(ds, data_dir))
    return paths


def load_jsonl(path: Path) -> list[dict]:
    """Load a JSONL file into a list of dicts."""
    docs = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                docs.append(json.loads(line))
    return docs


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    datasets = [int(x) for x in sys.argv[1:]] if len(sys.argv) > 1 else None
    download_all(datasets=datasets)
