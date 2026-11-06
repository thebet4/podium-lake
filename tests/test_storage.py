import json
import os

import pytest

from podium_lake.ingest import storage


@pytest.fixture
def folder(tmp_path):
    return tmp_path / "laps/season=2024/round=01"


def test_fetches_every_page_and_marks_done(api, folder):
    api.total = 250  # 3 pages of 100

    fetched, reused = storage.write_pages("2024/1/laps", folder)

    assert (fetched, reused) == (3, 0)
    assert api.offsets("/laps") == [0, 100, 200]
    assert storage.is_done(folder)


def test_resumes_without_repeating_saved_pages(api, folder):
    api.total = 1129  # 12 pages
    api.fail_on[("/laps", 800)] = RuntimeError("429 exhausted")

    with pytest.raises(RuntimeError):
        storage.write_pages("2024/1/laps", folder)
    assert not storage.is_done(folder)
    assert len(list(folder.glob("page_*.json"))) == 8  # pages 0..7 survived

    api.requests.clear()
    api.fail_on.clear()
    fetched, reused = storage.write_pages("2024/1/laps", folder)

    assert api.offsets("/laps") == [800, 900, 1000, 1100]
    assert (fetched, reused) == (4, 8)
    assert storage.is_done(folder)


def test_corrupt_page_is_fetched_again_and_neighbours_kept(api, folder):
    storage.write_pages("2024/1/laps", folder)
    (folder / "page_001.json").write_text('{"MRData": {"tot')

    api.requests.clear()
    storage.write_pages("2024/1/laps", folder)

    assert api.offsets("/laps") == [100]
    json.loads((folder / "page_001.json").read_text())  # valid again


def test_empty_page_file_is_fetched_again(api, folder):
    storage.write_pages("2024/1/laps", folder)
    (folder / "page_002.json").write_text("")

    api.requests.clear()
    storage.write_pages("2024/1/laps", folder)

    assert api.offsets("/laps") == [200]


def test_failed_write_never_leaves_a_truncated_page(api, folder, monkeypatch):
    real_replace = os.replace

    def failing_replace(source, destination):
        if str(destination).endswith("page_001.json"):
            raise OSError("disk full")
        return real_replace(source, destination)

    monkeypatch.setattr(storage.os, "replace", failing_replace)
    with pytest.raises(OSError):
        storage.write_pages("2024/1/laps", folder)
    monkeypatch.setattr(storage.os, "replace", real_replace)

    # only the finished page is a real page file; the interrupted one is a .tmp
    assert [p.name for p in folder.glob("page_*.json")] == ["page_000.json"]
    assert [p.name for p in folder.glob("*.tmp")] == ["page_001.json.tmp"]

    api.requests.clear()
    storage.write_pages("2024/1/laps", folder)

    assert api.offsets("/laps") == [100, 200]
    assert list(folder.glob("*.tmp")) == []
    assert storage.is_done(folder)


def test_force_discards_saved_pages(api, folder):
    storage.write_pages("2024/1/laps", folder)

    api.requests.clear()
    storage.write_pages("2024/1/laps", folder, force=True)

    assert api.offsets("/laps") == [0, 100, 200]


def test_saved_page_total_decides_when_to_stop(api, folder):
    storage.write_pages("2024/1/laps", folder)
    (folder / "_SUCCESS").unlink()
    (folder / "page_002.json").unlink()

    api.requests.clear()
    storage.write_pages("2024/1/laps", folder)

    # page 0 is read from disk, so its total (250) ends the loop after page 2
    assert api.offsets("/laps") == [200]
