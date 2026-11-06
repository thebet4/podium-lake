"""Silver layer: typed Parquet tables built from the bronze pages."""

from podium_lake.transform.silver import TABLES, build_silver

__all__ = ["TABLES", "build_silver"]
