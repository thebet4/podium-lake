"""Bronze page schemas, one per Jolpica endpoint, as Spark DDL strings.

Declared instead of inferred: every leaf is a string in the API, and fields
missing from some seasons (e.g. FastestLap before 2004) still resolve to null
instead of failing the select.
"""

_CIRCUIT = (
    "Circuit STRUCT<circuitId STRING, circuitName STRING,"
    " Location STRUCT<lat STRING, long STRING, locality STRING, country STRING>>"
)
_DRIVER = (
    "STRUCT<driverId STRING, permanentNumber STRING, code STRING, givenName STRING,"
    " familyName STRING, dateOfBirth STRING, nationality STRING>"
)
_CONSTRUCTOR = "STRUCT<constructorId STRING, name STRING, nationality STRING>"
_RESULT = (
    f"STRUCT<number STRING, position STRING, positionText STRING, points STRING,"
    f" Driver {_DRIVER}, Constructor {_CONSTRUCTOR}, grid STRING, laps STRING,"
    f" status STRING, Time STRUCT<millis STRING>,"
    f" FastestLap STRUCT<rank STRING, lap STRING, Time STRUCT<time STRING>>>"
)
_TIMING = "STRUCT<driverId STRING, position STRING, time STRING>"
_PIT_STOP = "STRUCT<driverId STRING, lap STRING, stop STRING, time STRING, duration STRING>"


def _races_schema(child: str = "") -> str:
    return (
        f"RaceTable STRUCT<Races ARRAY<STRUCT<season STRING, round STRING,"
        f" raceName STRING, date STRING, time STRING, {_CIRCUIT}{child}>>>"
    )


SCHEMAS = {
    "results": _races_schema(f", Results ARRAY<{_RESULT}>"),
    "laps": _races_schema(f", Laps ARRAY<STRUCT<number STRING, Timings ARRAY<{_TIMING}>>>"),
    "pitstops": _races_schema(f", PitStops ARRAY<{_PIT_STOP}>"),
    "races": _races_schema(),
    "drivers": f"DriverTable STRUCT<Drivers ARRAY<{_DRIVER}>>",
    "constructors": f"ConstructorTable STRUCT<Constructors ARRAY<{_CONSTRUCTOR}>>",
}
