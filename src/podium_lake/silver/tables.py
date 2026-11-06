from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import DataFrame

from podium_lake.silver import dimensions, facts


@dataclass(frozen=True)
class Table:
    """A silver table: which bronze endpoint it is built from, and how.

    Facts are `partition_by` season, so queries filtering a season read only
    its folder; dimensions are small and left unpartitioned (`[]`).
    """

    name: str
    endpoint: str
    build: Callable[[DataFrame], DataFrame]
    partition_by: list[str] = field(default_factory=list)


TABLES = {
    table.name: table
    for table in [
        Table("results", "results", facts.results_fact, ["season"]),
        Table("laps", "laps", facts.laps_fact, ["season"]),
        Table("pit_stops", "pitstops", facts.pit_stops_fact, ["season"]),
        Table("races", "races", dimensions.races_dim),
        Table("circuits", "races", dimensions.circuits_dim),
        Table("drivers", "drivers", dimensions.drivers_dim),
        Table("constructors", "constructors", dimensions.constructors_dim),
    ]
}
