import pytest

from podium_lake.ingest import http, runner


class FakeResponse:
    status_code = 200
    headers: dict = {}

    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self._data


class FakeApi:
    """Stands in for the Jolpica HTTP layer and records every real request.

    `total` is the number of rows the API reports; `fail_on` maps a
    (url suffix, offset) pair to an exception to raise on that request.
    """

    def __init__(self, rounds=(1,), total=250):
        self.rounds = list(rounds)
        self.total = total
        self.fail_on = {}
        self.requests = []  # (url, offset) of every request that reached the "API"

    def get(self, url, params=None):
        offset = params["offset"]
        self.requests.append((url, offset))

        for (suffix, fail_offset), error in self.fail_on.items():
            if url.endswith(suffix) and offset == fail_offset:
                raise error

        if url.endswith("/races"):
            races = [{"round": str(number)} for number in self.rounds]
            return FakeResponse({"MRData": {"total": str(len(races)), "RaceTable": {"Races": races}}})
        return FakeResponse({"MRData": {"total": str(self.total), "RaceTable": {"Races": [{"Laps": []}]}}})

    def offsets(self, suffix):
        """Offsets requested for URLs ending in `suffix`, in order."""
        return [offset for url, offset in self.requests if url.endswith(suffix)]


@pytest.fixture
def bronze(tmp_path, monkeypatch):
    """Redirect the Jolpica bronze folder to a temporary directory."""
    folder = tmp_path / "bronze/jolpica"
    monkeypatch.setattr(runner, "JOLPICA_DIR", folder)
    return folder


@pytest.fixture
def api(monkeypatch):
    fake = FakeApi()
    monkeypatch.setattr(http, "get_with_retry", fake.get)
    monkeypatch.setattr(http.settings, "api_url", "http://api")
    return fake
