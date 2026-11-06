import json
import shutil

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from make_sample import CONSTRUCTORS, DRIVERS, LAPS, ROUNDS, SEASONS, STOPS, make_sample
from podium_lake.transform.silver import TABLES, build_silver, to_millis

KEYS = {
    "results": ["season", "round", "driver_id"],
    "laps": ["season", "round", "driver_id", "lap"],
    "pit_stops": ["season", "round", "driver_id", "stop"],
    "races": ["season", "round"],
    "circuits": ["circuit_id"],
    "drivers": ["driver_id"],
    "constructors": ["constructor_id"],
}


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def built(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    bronze = make_sample(root / "bronze")
    silver = root / "silver"
    counts = build_silver(spark, str(bronze), str(silver))
    return bronze, silver, counts


def table(spark, built, name):
    return spark.read.parquet(str(built[1] / name))


def test_sample_pages_overlap(built):
    """Guard for the checkpoint below: without dedupe there would be more rows."""
    bronze = built[0]
    raw = sum(
        len(r["Results"])
        for page in (bronze / "results").rglob("page_*.json")
        for r in json.loads(page.read_text())["RaceTable"]["Races"]
    )
    assert raw > len(SEASONS) * ROUNDS * DRIVERS


def test_row_counts(built):
    races = len(SEASONS) * ROUNDS
    assert built[2] == {
        "results": races * DRIVERS,  # checkpoint: 3 x 22 x 20 = 1,320
        "laps": races * DRIVERS * LAPS,
        "pit_stops": races * DRIVERS * STOPS,
        "races": races,
        "circuits": ROUNDS,
        "drivers": DRIVERS,
        "constructors": CONSTRUCTORS,
    }
    assert built[2]["results"] == 1320


@pytest.mark.parametrize("name", TABLES)
def test_keys_are_unique(spark, built, name):
    df = table(spark, built, name)
    assert df.count() == df.select(*KEYS[name]).distinct().count()


def test_results_types_and_finished(spark, built):
    df = table(spark, built, "results")
    types = dict(df.dtypes)
    assert types["season"] == types["round"] == types["position"] == "int"
    assert types["points"] == "double"
    assert types["race_time_ms"] == "bigint"
    assert types["finished"] == "boolean"

    retired = df.filter(F.col("position_text") == "R")
    assert retired.count() == len(SEASONS) * ROUNDS
    assert retired.filter("finished or race_time_ms is not null").count() == 0
    assert df.filter("finished").count() == len(SEASONS) * ROUNDS * (DRIVERS - 1)
    assert df.filter("points = 0.5").count() == len(SEASONS) * ROUNDS


def test_dimension_types(spark, built):
    assert dict(table(spark, built, "races").dtypes)["date"] == "date"
    assert dict(table(spark, built, "drivers").dtypes)["date_of_birth"] == "date"
    assert dict(table(spark, built, "circuits").dtypes)["lat"] == "double"


@pytest.mark.parametrize("text, expected", [
    ("27.733", 27_733),
    ("1:31.929", 91_929),
    ("1:02:03.456", 3_723_456),
    (None, None),
])
def test_to_millis(spark, text, expected):
    df = spark.createDataFrame([(text,)], "t string")
    assert df.select(to_millis("t")).first()[0] == expected


@pytest.mark.parametrize("fact, dim, keys", [
    ("results", "drivers", ["driver_id"]),
    ("results", "constructors", ["constructor_id"]),
    ("results", "races", ["season", "round"]),
    ("laps", "drivers", ["driver_id"]),
    ("pit_stops", "races", ["season", "round"]),
])
def test_facts_reference_dimensions(spark, built, fact, dim, keys):
    orphans = table(spark, built, fact).join(table(spark, built, dim), keys, "left_anti")
    assert orphans.count() == 0


def test_only_facts_are_partitioned(built):
    silver = built[1]
    for name, (_, _, is_fact) in TABLES.items():
        partitions = sorted(p.name for p in (silver / name).glob("season=*"))
        if is_fact:
            assert partitions == [f"season={season}" for season in SEASONS]
            # repartition before partitionBy: one file per season
            assert all(len(list((silver / name / p).glob("*.parquet"))) == 1 for p in partitions)
        else:
            assert partitions == []
            assert len(list((silver / name).glob("*.parquet"))) == 1


@pytest.mark.parametrize("remove", ["folder", "pages"])
def test_missing_endpoint_is_skipped(spark, tmp_path, remove):
    bronze = make_sample(tmp_path / "bronze", seasons=(2024,))
    if remove == "folder":
        shutil.rmtree(bronze / "pitstops")
    else:
        for page in (bronze / "pitstops").rglob("page_*.json"):
            page.unlink()

    counts = build_silver(spark, str(bronze), str(tmp_path / "silver"))

    assert "pit_stops" not in counts
    assert counts["results"] == ROUNDS * DRIVERS
