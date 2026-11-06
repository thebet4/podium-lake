import logging

from podium_lake.ingest import runner, storage


def test_per_season_endpoint_is_written_under_its_name(api, bronze):
    summary = runner.ingest_by_season("drivers", 2024, 2024)

    assert summary.failed == []
    assert storage.is_done(bronze / "drivers/season=2024")


def test_second_run_makes_no_requests(api, bronze):
    runner.ingest_by_season("drivers", 2024, 2024)

    api.requests.clear()
    summary = runner.ingest_by_season("drivers", 2024, 2024)

    assert api.requests == []
    assert summary.skipped == 1
    assert summary.fetched == 0


def test_laps_take_their_rounds_from_races_without_refetching_them(api, bronze):
    api.rounds = [1, 2, 3]
    runner.ingest_by_season("races", 2024, 2024)

    api.requests.clear()
    summary = runner.ingest_by_season("laps", 2024, 2024)

    urls = sorted({url for url, _ in api.requests})
    assert urls == ["http://api/2024/1/laps", "http://api/2024/2/laps", "http://api/2024/3/laps"]
    assert summary.failed == []


def test_races_are_fetched_first_when_missing(api, bronze):
    api.rounds = [1, 2]

    runner.ingest_by_season("laps", 2024, 2024)

    assert api.requests[0][0] == "http://api/2024/races"
    assert storage.is_done(bronze / "races/season=2024")


def test_season_before_first_data_year_is_skipped(api, bronze, caplog):
    with caplog.at_level(logging.WARNING):
        runner.ingest_by_season("pitstops", 2010, 2010)

    assert api.requests == []
    assert "no data before 2011" in caplog.text


def test_one_failing_round_does_not_stop_the_season_and_is_resumable(api, bronze):
    api.rounds = [1, 2, 3]
    api.total = 1129
    api.fail_on[("2024/1/laps", 800)] = RuntimeError("429 exhausted")

    summary = runner.ingest_by_season("laps", 2024, 2024)

    assert summary.failed == ["2024 round 1"]
    assert storage.is_done(bronze / "laps/season=2024/round=02")
    assert storage.is_done(bronze / "laps/season=2024/round=03")
    assert not storage.is_done(bronze / "laps/season=2024/round=01")

    api.requests.clear()
    api.fail_on.clear()
    summary = runner.ingest_by_season("laps", 2024, 2024)

    # only the 4 missing pages of round 1; rounds 2 and 3 are skipped untouched
    assert api.offsets("2024/1/laps") == [800, 900, 1000, 1100]
    assert [url for url, _ in api.requests if not url.endswith("2024/1/laps")] == []
    assert (summary.fetched, summary.reused, summary.skipped) == (4, 8, 2)
    assert summary.failed == []


def test_keyboard_interrupt_is_not_swallowed(api, bronze):
    api.rounds = [1]
    api.fail_on[("2024/1/laps", 0)] = KeyboardInterrupt()

    try:
        runner.ingest_by_season("laps", 2024, 2024)
    except KeyboardInterrupt:
        return
    raise AssertionError("KeyboardInterrupt was swallowed")
