"""Write a synthetic bronze layer, shaped like the Jolpica pages, to test the silver layer offline.

Pages overlap: each page starts `limit - overlap` rows after the previous one,
so rows (and the race fields around them) appear in two pages, and races and
laps are split across pages, like the real API. The silver tables must still
have exactly one row per key.

Usage: python tests/make_sample.py <bronze folder>
"""

import json
import sys
from pathlib import Path

SEASONS = (2023, 2024, 2025)
ROUNDS = 22
DRIVERS = 20
CONSTRUCTORS = 10
LAPS = 5
STOPS = 2


def driver(index):
    return {
        "driverId": f"driver_{index:02d}",
        "permanentNumber": str(index + 1),
        "code": f"D{index:02d}",
        "givenName": f"Given{index}",
        "familyName": f"Family{index}",
        "dateOfBirth": f"199{index % 10}-01-{index + 1:02d}",
        "nationality": "Synthetic",
    }


def constructor(index):
    return {"constructorId": f"team_{index:02d}", "name": f"Team {index}", "nationality": "Synthetic"}


def race(season, round_number):
    return {
        "season": str(season),
        "round": str(round_number),
        "raceName": f"Grand Prix {round_number}",
        "date": f"{season}-{(round_number - 1) // 2 + 1:02d}-{round_number % 28 + 1:02d}",
        "time": "14:00:00Z",
        "Circuit": {
            "circuitId": f"circuit_{round_number:02d}",
            "circuitName": f"Circuit {round_number}",
            "Location": {"lat": "45.5", "long": "-9.25", "locality": "Town", "country": "Country"},
        },
    }


def result(index):
    """Result of driver `index`: the last driver retired, everyone else finished in order."""
    retired = index == DRIVERS - 1
    row = {
        "number": str(index + 1),
        "position": str(index + 1),
        "positionText": "R" if retired else str(index + 1),
        "points": "0.5" if index == 10 else str(max(0, 10 - index)),
        "Driver": driver(index),
        "Constructor": constructor(index // 2),
        "grid": str(DRIVERS - index),
        "laps": "40" if retired else "58",
        "status": "Retired" if retired else "Finished",
        "FastestLap": {"rank": str(index + 1), "lap": "40", "Time": {"time": f"1:{20 + index}.123"}},
    }
    if not retired:
        row["Time"] = {"millis": str(5_000_000 + index * 1000)}
    return row


def overlapping_pages(rows, limit, overlap):
    """Yield (offset, rows) slices of `limit` rows, each starting `limit - overlap` after the last."""
    offset = 0
    while True:
        yield offset, rows[offset:offset + limit]
        if offset + limit >= len(rows):
            return
        offset += limit - overlap


def group(rows, key, build):
    """Group consecutive `rows` with the same `key(row)` into `build(first row, rows)`."""
    groups = []
    for row in rows:
        if groups and key(groups[-1][0]) == key(row):
            groups[-1].append(row)
        else:
            groups.append([row])
    return [build(items[0], items) for items in groups]


def write_pages(folder, rows, table, to_page, limit, overlap):
    folder.mkdir(parents=True, exist_ok=True)
    for number, (offset, page_rows) in enumerate(overlapping_pages(rows, limit, overlap)):
        page = {
            "series": "f1", "limit": str(limit), "offset": str(offset), "total": str(len(rows)),
            table: to_page(page_rows),
        }
        (folder / f"page_{number:03d}.json").write_text(json.dumps(page))
    (folder / "_SUCCESS").touch()


def races_with(child):
    """Turn (race, item) rows into a RaceTable with the items under `child`."""
    def to_page(rows):
        races = group(rows, lambda row: row[0]["round"], lambda first, items: {
            **first[0], child: [item for _, item in items],
        })
        return {"Races": races}
    return to_page


def make_sample(root, seasons=SEASONS, limit=100, overlap=10):
    """Write the synthetic bronze for every endpoint under `root`; return `root`."""
    root = Path(root)

    for season in seasons:
        season_dir = f"season={season}"
        races = [race(season, number) for number in range(1, ROUNDS + 1)]

        write_pages(root / "races" / season_dir, races, "RaceTable",
                    lambda rows: {"Races": rows}, limit, overlap)
        write_pages(root / "drivers" / season_dir, [driver(i) for i in range(DRIVERS)], "DriverTable",
                    lambda rows: {"Drivers": rows}, limit, overlap)
        write_pages(root / "constructors" / season_dir, [constructor(i) for i in range(CONSTRUCTORS)],
                    "ConstructorTable", lambda rows: {"Constructors": rows}, limit, overlap)

        results = [(r, result(i)) for r in races for i in range(DRIVERS)]
        write_pages(root / "results" / season_dir, results, "RaceTable",
                    races_with("Results"), limit, overlap)

        for r in races:
            round_dir = root / "{}" / season_dir / f"round={int(r['round']):02d}"

            # Laps are paginated by timing rows, so a lap can be split across pages
            timings = [
                (r, (lap, {"driverId": f"driver_{i:02d}", "position": str(i + 1),
                           "time": f"1:{30 + i}.{lap:03d}"}))
                for lap in range(1, LAPS + 1) for i in range(DRIVERS)
            ]

            def laps_page(rows):
                races_page = races_with("Laps")(rows)
                for race_row in races_page["Races"]:
                    race_row["Laps"] = group(race_row["Laps"], lambda item: item[0], lambda first, items: {
                        "number": str(first[0]), "Timings": [timing for _, timing in items],
                    })
                return races_page

            write_pages(Path(str(round_dir).format("laps")), timings, "RaceTable", laps_page, limit, overlap)

            stops = [
                (r, {"driverId": f"driver_{i:02d}", "lap": str(15 * stop), "stop": str(stop),
                     "time": "14:30:00", "duration": f"2{stop}.{i:03d}"})
                for i in range(DRIVERS) for stop in range(1, STOPS + 1)
            ]
            write_pages(Path(str(round_dir).format("pitstops")), stops, "RaceTable",
                        races_with("PitStops"), limit // 3, overlap)

    return root


if __name__ == "__main__":
    print(make_sample(sys.argv[1] if len(sys.argv) > 1 else "data/sample/bronze/jolpica"))
