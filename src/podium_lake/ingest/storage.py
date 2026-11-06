import json
import logging
import os
from pathlib import Path

from podium_lake.ingest.http import paginate

logger = logging.getLogger(__name__)

SUCCESS_MARKER = "_SUCCESS"


def is_done(folder: Path) -> bool:
    """True if every page of `folder` was downloaded (the marker is written last)."""
    return (folder / SUCCESS_MARKER).exists()


def page_file(folder: Path, page_number: int) -> Path:
    return folder / f"page_{page_number:03d}.json"


def read_saved_page(folder, page_number):
    """Return a page already saved in `folder`, or None if missing or unreadable."""
    file_path = page_file(folder, page_number)
    if not file_path.exists():
        return None
    try:
        with open(file_path) as file:
            return json.load(file)
    except (json.JSONDecodeError, UnicodeDecodeError):
        # Never trust a corrupt file as a finished page: download it again
        logger.warning("Ignoring unreadable %s, it will be fetched again", file_path)
        return None


def save_page(folder, page_number, page):
    """Write a page atomically, so an existing page file is always complete."""
    file_path = page_file(folder, page_number)
    temp = file_path.with_name(file_path.name + ".tmp")
    with open(temp, "w") as file:
        json.dump(page, file, indent=4)
    os.replace(temp, file_path)


def write_pages(path, folder, force=False):
    """Fetch every page of `path` into `folder`, resuming from what is saved.

    Each page is its own file, written atomically. A page that is already on
    disk is not requested again, so an interrupted run (or a rate-limited one
    that gave up) continues where it stopped instead of starting over. The
    `_SUCCESS` marker is written only once every page is present.
    With `force=True` the saved pages are discarded first.

    Returns (pages fetched from the API, pages reused from disk).
    """
    folder.mkdir(parents=True, exist_ok=True)
    marker = folder / SUCCESS_MARKER
    marker.unlink(missing_ok=True)

    if force:
        for stale in folder.glob("page_*"):
            stale.unlink()

    fetched, reused = 0, 0
    for page_number, page, from_cache in paginate(
        path, cached=lambda number: read_saved_page(folder, number),
    ):
        if from_cache:
            reused += 1
        else:
            save_page(folder, page_number, page)
            fetched += 1

    marker.touch()
    return fetched, reused


def read_pages(folder):
    """Yield every saved page of `folder`, in order."""
    for file_path in sorted(folder.glob("page_*.json")):
        with open(file_path) as file:
            yield json.load(file)
