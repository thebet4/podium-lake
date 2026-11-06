"""Bronze layer: raw Jolpica API pages saved to disk.

Public API: `ingest_by_season` for any endpoint in `ENDPOINTS`, plus a
shortcut per endpoint.
"""

from podium_lake.ingest.endpoints import ENDPOINTS, Endpoint
from podium_lake.ingest.runner import Summary, ingest_by_season

__all__ = [
    "ENDPOINTS",
    "Endpoint",
    "Summary",
    "get_constructors_by_season",
    "get_drivers_by_season",
    "get_laps_by_season",
    "get_pitstops_by_season",
    "get_races_by_season",
    "get_results_by_season",
    "ingest_by_season",
]


def get_drivers_by_season(init_season=2026, end_season=2026, force=False):
    return ingest_by_season("drivers", init_season, end_season, force)


def get_constructors_by_season(init_season=2026, end_season=2026, force=False):
    return ingest_by_season("constructors", init_season, end_season, force)


def get_races_by_season(init_season=2026, end_season=2026, force=False):
    return ingest_by_season("races", init_season, end_season, force)


def get_results_by_season(init_season=2026, end_season=2026, force=False):
    return ingest_by_season("results", init_season, end_season, force)


def get_laps_by_season(init_season=2026, end_season=2026, force=False):
    return ingest_by_season("laps", init_season, end_season, force)


def get_pitstops_by_season(init_season=2026, end_season=2026, force=False):
    return ingest_by_season("pitstops", init_season, end_season, force)
