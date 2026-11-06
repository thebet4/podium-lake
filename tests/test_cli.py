from podium_lake import cli
from podium_lake.ingest import ENDPOINTS, Summary


def record_calls(monkeypatch, summary=None):
    """Replace the ingestion with a recorder, so the CLI is tested on its own."""
    calls = []

    def fake_ingest(name, init_season, end_season, force):
        calls.append((name, init_season, end_season, force))
        return summary or Summary()

    monkeypatch.setattr(cli, "ingest_by_season", fake_ingest)
    return calls


def test_single_endpoint_defaults_to_one_season(monkeypatch):
    calls = record_calls(monkeypatch)

    code = cli.main(["ingest", "drivers"])

    assert code == 0
    assert calls == [("drivers", 2026, 2026, False)]


def test_season_range_and_force_are_passed_through(monkeypatch):
    calls = record_calls(monkeypatch)

    cli.main(["ingest", "laps", "--from", "2024", "--to", "2026", "--force"])

    assert calls == [("laps", 2024, 2026, True)]


def test_to_defaults_to_from(monkeypatch):
    calls = record_calls(monkeypatch)

    cli.main(["ingest", "results", "--from", "2023"])

    assert calls == [("results", 2023, 2023, False)]


def test_all_runs_every_endpoint_with_races_before_the_ones_that_need_it(monkeypatch):
    calls = record_calls(monkeypatch)

    cli.main(["ingest", "all", "--from", "2024"])

    names = [name for name, *_ in calls]
    assert sorted(names) == sorted(ENDPOINTS)
    assert names.index("races") < names.index("laps")
    assert names.index("races") < names.index("pitstops")


def test_all_includes_endpoints_added_later_to_the_catalog(monkeypatch):
    calls = record_calls(monkeypatch)
    # the CLI reads the catalog at call time, so a new endpoint needs no CLI change
    monkeypatch.setattr(cli, "ENDPOINTS", {**ENDPOINTS, "circuits": ENDPOINTS["drivers"]})

    cli.main(["ingest", "all"])

    assert "circuits" in [name for name, *_ in calls]


def test_exit_code_is_non_zero_when_something_failed(monkeypatch):
    record_calls(monkeypatch, Summary(failed=["2024 round 3"]))

    assert cli.main(["ingest", "laps"]) == 1


def test_one_failing_endpoint_in_all_still_fails_the_run(monkeypatch):
    calls = []

    def fake_ingest(name, init_season, end_season, force):
        calls.append(name)
        return Summary(failed=["2024 round 3"]) if name == "laps" else Summary()

    monkeypatch.setattr(cli, "ingest_by_season", fake_ingest)

    assert cli.main(["ingest", "all"]) == 1
    assert sorted(calls) == sorted(ENDPOINTS)  # the others still ran after laps


def test_rejects_unknown_endpoint_and_inverted_range():
    for argv in (["ingest", "foo"], ["ingest", "drivers", "--from", "2026", "--to", "2024"]):
        try:
            cli.main(argv)
        except SystemExit as exit_info:
            assert exit_info.code == 2
        else:
            raise AssertionError(f"{argv} was accepted")
