import logging

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from podium_lake.silver.schemas import SCHEMAS
from podium_lake.silver.tables import TABLES, Table

logger = logging.getLogger(__name__)


def read_endpoint(spark: SparkSession, bronze: str, name: str) -> DataFrame | None:
    """Read every page of one bronze endpoint, or None if it was not ingested.

    Adds `_source` (the page's file path) so `dedupe` can prefer the latest page.
    """
    try:
        df = (
            spark.read.schema(SCHEMAS[name])
            .option("multiLine", True)
            .option("recursiveFileLookup", True)  # season=/round= are read from the JSON
            .option("pathGlobFilter", "page_*.json")  # skip _SUCCESS and *.tmp
            .json(f"{bronze}/{name}")
        )
    except AnalysisException as error:
        logger.warning("Bronze '%s' not found, skipping (%s)", name, error.getCondition())
        return None
    if df.isEmpty():
        logger.warning("Bronze '%s' has no pages, skipping", name)
        return None
    logger.debug("Bronze '%s' schema:\n%s", name, df.schema.treeString())
    return df.withColumn("_source", F.col("_metadata.file_path"))


def write_table(df: DataFrame, table: Table, path: str) -> None:
    """Write `df` as Parquet, one file per partition (or a single file if unpartitioned).

    Repartitioning by the partition columns first avoids one small file per
    Spark task in every partition folder.
    """
    if table.partition_by:
        df = df.repartition(*table.partition_by)
    else:
        df = df.coalesce(1)
    df.write.mode("overwrite").partitionBy(*table.partition_by).parquet(path)


def build_silver(spark: SparkSession, bronze: str, silver: str) -> dict[str, int]:
    """Build every silver table from `bronze` and write it as Parquet under `silver`.

    `bronze` and `silver` are plain path strings, so they can be local folders
    or `s3://` prefixes (the Glue job reuses this function as is). An endpoint
    missing from bronze is skipped. Returns the row count of each table written.
    """
    sources: dict[str, DataFrame | None] = {}
    counts = {}

    for table in TABLES.values():
        if table.endpoint not in sources:
            sources[table.endpoint] = read_endpoint(spark, bronze, table.endpoint)
        source = sources[table.endpoint]
        if source is None:
            continue

        path = f"{silver}/{table.name}"
        write_table(table.build(source), table, path)
        counts[table.name] = spark.read.parquet(path).count()
        logger.info("Silver '%s': %d row(s) written to %s", table.name, counts[table.name], path)

    return counts
