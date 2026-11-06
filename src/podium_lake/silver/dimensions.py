"""Dimension tables: one row per entity (a race, a circuit, a driver, a constructor)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from podium_lake.silver.columns import dedupe, explode_races, race_keys


def races_dim(df: DataFrame) -> DataFrame:
    return dedupe(
        explode_races(df).select(
            "_source",
            *race_keys(),
            F.col("race.raceName").alias("race_name"),
            F.to_date("race.date").alias("date"),
            F.col("race.time").alias("time"),
            F.col("race.Circuit.circuitId").alias("circuit_id"),
        ),
        ["season", "round"],
    )


def circuits_dim(df: DataFrame) -> DataFrame:
    circuit = F.col("race.Circuit")
    return dedupe(
        explode_races(df).select(
            "_source",
            circuit.circuitId.alias("circuit_id"),
            circuit.circuitName.alias("circuit_name"),
            circuit.Location.locality.alias("locality"),
            circuit.Location.country.alias("country"),
            circuit.Location.lat.cast("double").alias("lat"),
            circuit.Location.long.cast("double").alias("long"),
        ),
        ["circuit_id"],
    )


def drivers_dim(df: DataFrame) -> DataFrame:
    d = F.col("d")
    return dedupe(
        df.select("_source", F.explode("DriverTable.Drivers").alias("d")).select(
            "_source",
            d.driverId.alias("driver_id"),
            d.code.alias("code"),
            d.permanentNumber.cast("int").alias("permanent_number"),
            d.givenName.alias("given_name"),
            d.familyName.alias("family_name"),
            F.to_date(d.dateOfBirth).alias("date_of_birth"),
            d.nationality.alias("nationality"),
        ),
        ["driver_id"],
    )


def constructors_dim(df: DataFrame) -> DataFrame:
    c = F.col("c")
    return dedupe(
        df.select("_source", F.explode("ConstructorTable.Constructors").alias("c")).select(
            "_source",
            c.constructorId.alias("constructor_id"),
            c["name"].alias("name"),
            c.nationality.alias("nationality"),
        ),
        ["constructor_id"],
    )
