"""Silver layer: typed Parquet tables built from the raw Jolpica pages.

A star schema: facts (`results`, `laps`, `pit_stops`) partitioned by season,
and dimensions (`races`, `circuits`, `drivers`, `constructors`). The single
entry point is `build_silver(spark, bronze, silver)`, reused by the Glue job.
"""

from podium_lake.silver.runner import build_silver
from podium_lake.silver.tables import TABLES, Table

__all__ = ["TABLES", "Table", "build_silver"]
