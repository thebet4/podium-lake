from dataclasses import dataclass


@dataclass(frozen=True)
class Endpoint:
    """How a Jolpica endpoint is addressed.

    `per_round` endpoints need the round in the path (`/{season}/{round}/laps`);
    the others are served for the whole season (`/{season}/drivers`).
    `min_season` is the first season the API has data for.
    """

    name: str
    per_round: bool = False
    min_season: int = 1950


ENDPOINTS = {
    "drivers": Endpoint("drivers"),
    "constructors": Endpoint("constructors"),
    "races": Endpoint("races"),
    "results": Endpoint("results"),
    "laps": Endpoint("laps", per_round=True, min_season=1996),
    "pitstops": Endpoint("pitstops", per_round=True, min_season=2011),
}
