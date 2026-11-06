import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

from podium_lake.client import paginate
from podium_lake.logging_config import setup_logging

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
BRONZE_DIR = DATA_DIR / "bronze/jolpica"
SUCCESS_MARKER = "_SUCCESS"


@dataclass(frozen=True)
class Endpoint:
    """How a Jolpica endpoint is addressed.

    `per_round` endpoints need the round in the path (`/{season}/{round}/laps`);
    the others are served for the whole season (`/{season}/drivers`).
    `min_season` is the first season the API has data for.
    """

    name: str
    per_round: bool = False
    min_season: int = 1950


ENDPOINTS = {
    "drivers": Endpoint("drivers"),
    "races": Endpoint("races"),
    "results": Endpoint("results"),
    "laps": Endpoint("laps", per_round=True, min_season=1996),
    "pitstops": Endpoint("pitstops", per_round=True, min_season=2011),
}


def _is_done(folder: Path) -> bool:
    return (folder / SUCCESS_MARKER).exists()


def _page_file(folder: Path, page_number: int) -> Path:
    return folder / f"page_{page_number:03d}.json"


def _read_saved_page(folder, page_number):
    """Return a page already saved in `folder`, or None if missing or unreadable."""
    file_path = _page_file(folder, page_number)
    if not file_path.exists():
        return None
    try:
        with open(file_path) as file:
            return json.load(file)
    except (json.JSONDecodeError, UnicodeDecodeError):
        # Never trust a corrupt file as a finished page: download it again
        logger.warning("Ignoring unreadable %s, it will be fetched again", file_path)
        return None


def _save_page(folder, page_number, page):
    """Write a page atomically, so an existing page file is always complete."""
    file_path = _page_file(folder, page_number)
    temp = file_path.with_name(file_path.name + ".tmp")
    with open(temp, "w") as file:
        json.dump(page, file, indent=4)
    os.replace(temp, file_path)


def _write_pages(path, folder, force=False):
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
        path, cached=lambda number: _read_saved_page(folder, number),
    ):
        if from_cache:
            reused += 1
        else:
            _save_page(folder, page_number, page)
            fetched += 1

    marker.touch()
    return fetched, reused


def _rounds_of_season(season):
    """Return the round numbers of `season`, reading the `races` bronze data.

    Uses the files already on disk to save API requests, and ingests `races`
    first if they are missing.
    """
    folder = BRONZE_DIR / f"races/season={season}"
    if not _is_done(folder):
        logger.info("Season %d: 'races' not in bronze yet, fetching it to list the rounds", season)
        _write_pages(f"{season}/races", folder)

    rounds = []
    for page_file in sorted(folder.glob("page_*.json")):
        with open(page_file) as file:
            page = json.load(file)
        rounds.extend(int(race["round"]) for race in page["RaceTable"]["Races"])
    return sorted(set(rounds))


@dataclass
class Summary:
    """What one ingestion run did.

    `fetched` is the number of API requests that produced a page (what counts
    against the rate limit); `reused` is pages already on disk.
    """

    fetched: int = 0
    reused: int = 0
    skipped: int = 0  # folders already complete, not even opened
    failed: list = field(default_factory=list)

    def add(self, other):
        self.fetched += other.fetched
        self.reused += other.reused
        self.skipped += other.skipped
        self.failed.extend(other.failed)


def _ingest_folder(path, folder, label, force):
    """Ingest one folder (a season, or a round of a season) into the bronze layer."""
    if not force and _is_done(folder):
        logger.info("%s: already complete, skipping", label)
        return Summary(skipped=1)

    fetched, reused = _write_pages(path, folder, force)
    if reused:
        logger.info("%s: %d page(s) fetched, %d reused from disk", label, fetched, reused)
    else:
        logger.info("%s: %d page(s) fetched", label, fetched)
    return Summary(fetched=fetched, reused=reused)


def _ingest_season(endpoint, season, force):
    if not endpoint.per_round:
        folder = BRONZE_DIR / f"{endpoint.name}/season={season}"
        return _ingest_folder(f"{season}/{endpoint.name}", folder, f"Season {season} '{endpoint.name}'", force)

    rounds = _rounds_of_season(season)
    logger.info("Season %d: '%s' for %d round(s)", season, endpoint.name, len(rounds))
    summary = Summary()

    for index, round_number in enumerate(rounds, start=1):
        label = f"[{index}/{len(rounds)}] {season} round {round_number} '{endpoint.name}'"
        folder = BRONZE_DIR / f"{endpoint.name}/season={season}/round={round_number:02d}"
        try:
            summary.add(_ingest_folder(f"{season}/{round_number}/{endpoint.name}", folder, label, force))
        except Exception as error:  # noqa: BLE001
            # One failing round (e.g. rate limit exhausted) must not stop the season.
            # Its pages already saved stay on disk and are reused on the next run.
            logger.error("%s: failed (%s: %s)", label, type(error).__name__, error)
            summary.failed.append(f"{season} round {round_number}")

    return summary


def ingest_by_season(endpoint_name, init_season=2026, end_season=2026, force=False):
    """Write the raw pages of `endpoint_name` for each season to the bronze layer.

    Resumable: pages already saved are not requested again, and folders that
    are already complete are skipped, so an interrupted or rate-limited run
    can simply be run again. `force=True` discards what is saved and starts over.
    """
    endpoint = ENDPOINTS[endpoint_name]
    logger.info("Starting ingestion of '%s' for seasons %d-%d", endpoint.name, init_season, end_season)
    started = time.monotonic()
    total = Summary()

    for season in range(init_season, end_season + 1):
        if season < endpoint.min_season:
            logger.warning(
                "Season %d: '%s' has no data before %d, skipping",
                season, endpoint.name, endpoint.min_season,
            )
            continue
        total.add(_ingest_season(endpoint, season, force))

    logger.info(
        "Finished '%s': %d request(s), %d page(s) reused, %d folder(s) skipped, %d failed in %.1fs",
        endpoint.name, total.fetched, total.reused, total.skipped, len(total.failed),
        time.monotonic() - started,
    )
    if total.failed:
        logger.warning("'%s' incomplete, run again to resume: %s", endpoint.name, "; ".join(total.failed))
    return total


def get_drivers_by_season(init_season=2026, end_season=2026, force=False):
    return ingest_by_season("drivers", init_season, end_season, force)


def get_races_by_season(init_season=2026, end_season=2026, force=False):
    return ingest_by_season("races", init_season, end_season, force)


def get_results_by_season(init_season=2026, end_season=2026, force=False):
    return ingest_by_season("results", init_season, end_season, force)


def get_laps_by_season(init_season=2026, end_season=2026, force=False):
    return ingest_by_season("laps", init_season, end_season, force)


def get_pitstops_by_season(init_season=2026, end_season=2026, force=False):
    return ingest_by_season("pitstops", init_season, end_season, force)


if __name__ == "__main__":
    setup_logging()
    get_drivers_by_season(2026, 2026)
    get_races_by_season(2026, 2026)
    get_results_by_season(2026, 2026)
    get_laps_by_season(2026, 2026)
    get_pitstops_by_season(2026, 2026)
