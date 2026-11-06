"""Command line entry point.

`podium-lake ingest <endpoint> --from 2024 --to 2026` fills the bronze layer;
`podium-lake transform` builds the silver layer from it.
"""

import argparse
import logging
import sys
from collections.abc import Sequence

from podium_lake.ingest import ENDPOINTS, Summary, ingest_by_season
from podium_lake.logging_config import setup_logging
from podium_lake.paths import BRONZE_DIR, SILVER_DIR

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

    transform = commands.add_parser(
        "transform",
        help="build the silver Parquet tables from the bronze layer",
        description=f"Build the silver Parquet tables from {BRONZE_DIR} into {SILVER_DIR}.",
    )
    transform.add_argument(
        "-v", "--verbose", action="store_true",
        help="log the bronze schemas read",
    )
    return parser


def run_ingest(endpoint, init_season, end_season, force) -> Summary:
    names = endpoint_order() if endpoint == ALL else [endpoint]
    total = Summary()
    for name in names:
        total.add(ingest_by_season(name, init_season, end_season, force))
    return total


def run_transform() -> dict[str, int]:
    # Imported here so `ingest` does not pay for starting the JVM
    from pyspark.sql import SparkSession

    from podium_lake.transform import build_silver

    spark = SparkSession.builder.appName("podium-lake").master("local[*]").getOrCreate()
    try:
        return build_silver(spark, str(BRONZE_DIR / "jolpica"), str(SILVER_DIR))
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "ingest":
        if args.end_season is None:
            args.end_season = args.init_season
        if args.end_season < args.init_season:
            parser.error(f"--to ({args.end_season}) is before --from ({args.init_season})")

    setup_logging(logging.DEBUG if args.verbose else logging.INFO)

    if args.command == "transform":
        run_transform()
        return 0

    if args.command == "ingest":
        summary = run_ingest(args.endpoint, args.init_season, args.end_season, args.force)
        # Non-zero when something is missing, so a wrapper can loop until it is complete
        return 1 if summary.failed else 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
