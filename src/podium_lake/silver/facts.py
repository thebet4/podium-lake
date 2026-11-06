"""Fact tables: one row per event (a result, a lap, a pit stop)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from podium_lake.silver.columns import dedupe, explode_races, race_keys, to_millis


def results_fact(df: DataFrame) -> DataFrame:
    """One row per driver per race."""
    rows = explode_races(df).select("_source", "race", F.explode("race.Results").alias("r"))
    return dedupe(
        rows.select(
            "_source",
            *race_keys(),
            F.col("r.Driver.driverId").alias("driver_id"),
            F.col("r.Constructor.constructorId").alias("constructor_id"),
            F.col("r.number").cast("int").alias("number"),
            F.col("r.grid").cast("int").alias("grid"),
            F.col("r.position").cast("int").alias("position"),
            F.col("r.positionText").alias("position_text"),
            # Numeric positionText = classified; letters are R(etired), D(isqualified), W(ithdrawn)...
            F.col("r.positionText").rlike("^[0-9]+$").alias("finished"),
            F.col("r.points").cast("double").alias("points"),
            F.col("r.laps").cast("int").alias("laps"),
            F.col("r.status").alias("status"),
            F.col("r.Time.millis").cast("long").alias("race_time_ms"),
            F.col("r.FastestLap.rank").cast("int").alias("fastest_lap_rank"),
            F.col("r.FastestLap.lap").cast("int").alias("fastest_lap_number"),
            to_millis("r.FastestLap.Time.time").alias("fastest_lap_time_ms"),
        ),
        ["season", "round", "driver_id"],
    )


def laps_fact(df: DataFrame) -> DataFrame:
    """One row per driver per lap of a race."""
    laps = explode_races(df).select("_source", "race", F.explode("race.Laps").alias("lap"))
    rows = laps.select("_source", "race", "lap", F.explode("lap.Timings").alias("t"))
    return dedupe(
        rows.select(
            "_source",
            *race_keys(),
            F.col("t.driverId").alias("driver_id"),
            F.col("lap.number").cast("int").alias("lap"),
            F.col("t.position").cast("int").alias("position"),
            to_millis("t.time").alias("lap_time_ms"),
        ),
        ["season", "round", "driver_id", "lap"],
    )


def pit_stops_fact(df: DataFrame) -> DataFrame:
    """One row per pit stop."""
    rows = explode_races(df).select("_source", "race", F.explode("race.PitStops").alias("p"))
    return dedupe(
        rows.select(
            "_source",
            *race_keys(),
            F.col("p.driverId").alias("driver_id"),
            F.col("p.stop").cast("int").alias("stop"),
            F.col("p.lap").cast("int").alias("lap"),
            F.col("p.time").alias("time_of_day"),
            to_millis("p.duration").alias("duration_ms"),
        ),
        ["season", "round", "driver_id", "stop"],
    )
