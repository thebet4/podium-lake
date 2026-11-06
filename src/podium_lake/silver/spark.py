from pyspark.sql import SparkSession


def local_session(app_name: str = "podium-lake", cores: str = "*") -> SparkSession:
    """A local SparkSession for the CLI and tests (Glue provides its own)."""
    return (
        SparkSession.builder.appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.ui.enabled", "false")
        # The tables are small: Spark's default of 200 shuffle partitions is pure overhead
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
