import logging
import time
from dataclasses import dataclass, field

from podium_lake.ingest import storage
from podium_lake.ingest.endpoints import ENDPOINTS
from podium_lake.paths import BRONZE_DIR

logger = logging.getLogger(__name__)

JOLPICA_DIR = BRONZE_DIR / "jolpica"


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


def _rounds_of_season(season):
    """Return the round numbers of `season`, reading the `races` bronze data.

    Uses the files already on disk to save API requests, and ingests `races`
    first if they are missing.
    """
    folder = JOLPICA_DIR / f"races/season={season}"
    if not storage.is_done(folder):
        logger.info("Season %d: 'races' not in bronze yet, fetching it to list the rounds", season)
        storage.write_pages(f"{season}/races", folder)

    rounds = []
    for page in storage.read_pages(folder):
        rounds.extend(int(race["round"]) for race in page["RaceTable"]["Races"])
    return sorted(set(rounds))


def _ingest_folder(path, folder, label, force):
    """Ingest one folder (a season, or a round of a season) into the bronze layer."""
    if not force and storage.is_done(folder):
        logger.info("%s: already complete, skipping", label)
        return Summary(skipped=1)

    fetched, reused = storage.write_pages(path, folder, force)
    if reused:
        logger.info("%s: %d page(s) fetched, %d reused from disk", label, fetched, reused)
    else:
        logger.info("%s: %d page(s) fetched", label, fetched)
    return Summary(fetched=fetched, reused=reused)


def _ingest_season(endpoint, season, force):
    if not endpoint.per_round:
        folder = JOLPICA_DIR / f"{endpoint.name}/season={season}"
        return _ingest_folder(f"{season}/{endpoint.name}", folder, f"Season {season} '{endpoint.name}'", force)

    rounds = _rounds_of_season(season)
    logger.info("Season %d: '%s' for %d round(s)", season, endpoint.name, len(rounds))
    summary = Summary()

    for index, round_number in enumerate(rounds, start=1):
        label = f"[{index}/{len(rounds)}] {season} round {round_number} '{endpoint.name}'"
        folder = JOLPICA_DIR / f"{endpoint.name}/season={season}/round={round_number:02d}"
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
