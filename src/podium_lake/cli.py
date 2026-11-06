"""Command line entry point: `podium-lake ingest <endpoint> --from 2024 --to 2026`."""

import argparse
import logging
import sys
from collections.abc import Sequence

from podium_lake.ingest import ENDPOINTS, Summary, ingest_by_season
from podium_lake.logging_config import setup_logging

DEFAULT_SEASON = 2026
ALL = "all"

# Run order for `ingest all`: `races` comes before the endpoints that need its rounds.
# Any endpoint added to ENDPOINTS later is appended, so it is never silently left out.
_PREFERRED_ORDER = ["drivers", "constructors", "races", "results", "laps", "pitstops"]


def endpoint_order() -> list[str]:
    ordered = [name for name in _PREFERRED_ORDER if name in ENDPOINTS]
    ordered += [name for name in ENDPOINTS if name not in ordered]
    return ordered


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="podium-lake",
        description="Podium Lake data pipeline.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    ingest = commands.add_parser(
        "ingest",
        help="download raw Jolpica pages into the bronze layer",
        description=(
            "Download raw Jolpica pages into the bronze layer. Resumable: pages already "
            "saved are not requested again, so a run that failed or was rate limited can "
            "simply be run again."
        ),
    )
    ingest.add_argument(
        "endpoint",
        choices=[*ENDPOINTS, ALL],
        help="what to ingest; 'all' runs every endpoint",
    )
    ingest.add_argument(
        "--from", dest="init_season", type=int, default=DEFAULT_SEASON,
        metavar="SEASON", help=f"first season (default: {DEFAULT_SEASON})",
    )
    ingest.add_argument(
        "--to", dest="end_season", type=int, default=None,
        metavar="SEASON", help="last season (default: same as --from)",
    )
    ingest.add_argument(
        "--force", action="store_true",
        help="discard what is already saved and download again",
    )
    ingest.add_argument(
        "-v", "--verbose", action="store_true",
        help="log every request and file written",
    )
    return parser


def run_ingest(endpoint, init_season, end_season, force) -> Summary:
    names = endpoint_order() if endpoint == ALL else [endpoint]
    total = Summary()
    for name in names:
        total.add(ingest_by_season(name, init_season, end_season, force))
    return total


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.end_season is None:
        args.end_season = args.init_season
    if args.end_season < args.init_season:
        build_parser().error(f"--to ({args.end_season}) is before --from ({args.init_season})")

    setup_logging(logging.DEBUG if args.verbose else logging.INFO)

    if args.command == "ingest":
        summary = run_ingest(args.endpoint, args.init_season, args.end_season, args.force)
        # Non-zero when something is missing, so a wrapper can loop until it is complete
        return 1 if summary.failed else 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
