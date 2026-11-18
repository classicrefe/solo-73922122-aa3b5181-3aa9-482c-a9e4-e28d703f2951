"""读数封存：CSV 内容、快照隔离、独立版本与失败原子性。"""
from __future__ import annotations

import csv
import hashlib
import io
import threading
import time

import psycopg
from fastapi.testclient import TestClient

DAY = "2026-11-15"
DAY_START = "TIMESTAMPTZ '2026-11-15 00:00:00+00'"
DAY_END = "TIMESTAMPTZ '2026-11-16 00:00:00+00'"


def _create(client: TestClient, day: str = DAY) -> dict:
    resp = client.post("/archives", json={"date": day})
    assert resp.status_code == 201, resp.text
    return resp.json()


def _download(client: TestClient, archive_id: str) -> bytes:
    resp = client.get(f"/archives/{archive_id}/download")
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("text/csv")
    return resp.content


def _read_csv(data: bytes) -> list[list[str]]:
    return list(csv.reader(io.StringIO(data.decode("utf-8"))))


def _day_rows_before(pg_dsn: str) -> list[tuple]:
    """固定日期区间的基线读数（其他用例可能改动过两侧之外的日期）。"""
    return _db_readings(pg_dsn)


def _db_readings(pg_dsn: str) -> list[tuple]:
    conn = psycopg.connect(pg_dsn, autocommit=True)
    try:
        return conn.execute(
            "SELECT meter_id, to_char(read_time AT TIME ZONE 'UTC', "
            "'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'), wh "
            "FROM readings WHERE read_time >= " + DAY_START +
            " AND read_time < " + DAY_END + " ORDER BY meter_id, read_time"
        ).fetchall()
    finally:
        conn.close()


def test_archive_writes_csv_with_header_counts_and_sha(
    client: TestClient, archive_env, pg_dsn: str
) -> None:
    body = _create(client)
    data = _download(client, body["archive_id"])

    # 返回的 SHA-256 / 行数 / 大小与实际文件一致
    assert hashlib.sha256(data).hexdigest() == body["file_sha256"]
    assert len(data) == body["file_size"]
    rows = _read_csv(data)
    assert rows[0] == ["meter_id", "read_time", "wh"]
    assert len(rows) - 1 == body["row_count"]

    # 内容与源表该 UTC 日期区间逐行一致（按表号、时刻排序）
    expected = [
        [str(m), ts, str(wh)] for m, ts, wh in _db_readings(pg_dsn)
    ]
    assert rows[1:] == expected
    assert all(r[1].endswith("Z") for r in rows[1:])
    assert body["archive_date"] == DAY
    assert body["download_url"] == f"/archives/{body['archive_id']}/download"

    # 文件落盘目录取自配置，且没有残留临时文件
    _fresh, directory = archive_env
    files = sorted(p.name for p in directory.iterdir())
    assert len(files) == 1 and files[0].endswith(".csv")


def test_archive_is_snapshot_consistent_during_concurrent_correction(
    client: TestClient, archive_env, pg_dsn: str, monkeypatch
) -> None:
    """封存期间独立 SQL 更正源表并提交；本次文件仍只含原快照读数。"""
    manager, _directory = archive_env
    original = _day_rows_before(pg_dsn)

    # 门控：快照已打开、查询尚未执行时暂停，确保 UPDATE 发生在两者之间
    snapshot_open = threading.Event()
    proceed = threading.Event()
    real_stream = manager._stream_to_temp

    def gated(conn, day, fd):
        snapshot_open.set()
        assert proceed.wait(30), "测试未放行封存"
        return real_stream(conn, day, fd)

    monkeypatch.setattr(manager, "_stream_to_temp", gated)
    result: dict = {}

    def do_archive() -> None:
        result["body"] = _create(client)

    t = threading.Thread(target=do_archive)
    t.start()
    assert snapshot_open.wait(10), "快照未打开"

    writer = psycopg.connect(pg_dsn, autocommit=True)
    try:
        writer.execute(
            "UPDATE readings SET wh = wh + 1000 WHERE read_time >= "
            + DAY_START + " AND read_time < " + DAY_END
        )
        writer.execute(
            "INSERT INTO readings VALUES "
            "(1001, TIMESTAMPTZ '2026-11-15 12:00:00+00', 424242) "
            "ON CONFLICT DO NOTHING"
        )
        proceed.set()
        t.join(timeout=30)
        assert not t.is_alive()

        body = result["body"]
        rows = _read_csv(_download(client, body["archive_id"]))
        # 更正已提交，但本次文件只含快照打开时的读数
        assert rows[1:] == [[str(m), ts, str(wh)] for m, ts, wh in original]
        assert len(rows) - 1 == len(original) == body["row_count"]
        assert not any(r[1].startswith("2026-11-15T12:00:00") for r in rows[1:])

        # 源表仍处于更正后状态，再次封存形成独立版本
        monkeypatch.setattr(manager, "_stream_to_temp", real_stream)
        body2 = _create(client)
        assert body2["archive_id"] != body["archive_id"]
        rows2 = _read_csv(_download(client, body2["archive_id"]))
        assert len(rows2) - 1 == len(original) + 1
        assert any(r[1].startswith("2026-11-15T12:00:00") for r in rows2[1:])
        assert {tuple(r) for r in rows2[1:]} != {tuple(r) for r in rows[1:]}
        # 旧版本内容不变，两次封存都能下载
        assert _read_csv(_download(client, body["archive_id"])) == rows
        listed = client.get("/archives", params={"date": DAY}).json()
        assert {a["archive_id"] for a in listed} >= {
            body["archive_id"], body2["archive_id"]
        }
    finally:
        proceed.set()
        writer.execute(
            "UPDATE readings SET wh = wh - 1000 WHERE read_time >= "
            + DAY_START + " AND read_time < " + DAY_END
        )
        writer.execute(
            "DELETE FROM readings WHERE meter_id = 1001 "
            "AND read_time = TIMESTAMPTZ '2026-11-15 12:00:00+00'"
        )
        writer.close()


def test_download_returns_410_when_file_missing(client: TestClient, archive_env) -> None:
    body = _create(client)
    _fresh, directory = archive_env
    for path in directory.glob("*.csv"):
        path.unlink()

    resp = client.get(f"/archives/{body['archive_id']}/download")
    assert resp.status_code == 410
    # 元数据仍在，但下载绝不返回半份内容
    assert client.get(f"/archives/{body['archive_id']}").status_code == 200


def test_download_unknown_id_is_404(client: TestClient, archive_env) -> None:
    assert client.get("/archives/deadbeef").status_code == 404
    assert client.get("/archives/deadbeef/download").status_code == 404


def test_empty_day_produces_header_only_csv(client: TestClient, archive_env) -> None:
    body = _create(client, "1999-01-01")
    data = _download(client, body["archive_id"])
    assert body["row_count"] == 0
    assert data.decode("utf-8") == "meter_id,read_time,wh\n"
    assert hashlib.sha256(data).hexdigest() == body["file_sha256"]
    assert len(data) == body["file_size"]


def test_registration_failure_leaves_no_downloadable_file(
    client: TestClient, archive_env, monkeypatch, pg_dsn: str
) -> None:
    """登记数据库失败：不留正式文件、不产生可下载记录，重试仍可成功。"""
    from app import archives as archives_mod

    manager, directory = archive_env
    real_insert = manager._insert_record

    def fail_insert(**_kwargs):
        raise psycopg.OperationalError("simulated registry outage")

    # 每个用例从干净的元数据表开始（共享数据库）
    admin = psycopg.connect(pg_dsn, autocommit=True)
    try:
        admin.execute("TRUNCATE reading_archives")
    finally:
        admin.close()

    monkeypatch.setattr(manager, "_insert_record", fail_insert)
    resp = client.post("/archives", json={"date": DAY})
    assert resp.status_code == 503
    assert list(directory.glob("*.csv")) == []
    assert list(directory.glob(".part-*")) == []
    assert client.get("/archives").json() == []

    # 恢复登记后重试即可封存
    monkeypatch.setattr(manager, "_insert_record", real_insert)
    body = _create(client)
    assert body["row_count"] >= 1
    _ = archives_mod


def test_write_failure_leaves_no_record_or_file(
    client: TestClient, archive_env, monkeypatch, pg_dsn: str
) -> None:
    """写文件失败：无记录、无残留、可重试。"""
    manager, directory = archive_env
    admin = psycopg.connect(pg_dsn, autocommit=True)
    try:
        admin.execute("TRUNCATE reading_archives")
    finally:
        admin.close()

    real_stream = manager._stream_to_temp

    def boom(_conn, _day, _fd):
        import os

        os.close(_fd)  # 模拟写入端已关闭句柄后失败
        raise OSError("disk full (simulated)")

    monkeypatch.setattr(manager, "_stream_to_temp", boom)
    resp = client.post("/archives", json={"date": DAY})
    assert resp.status_code == 503
    assert list(directory.iterdir()) == []
    assert client.get("/archives").json() == []

    # 故障排除后重试即可成功，且不残留半份文件
    monkeypatch.setattr(manager, "_stream_to_temp", real_stream)
    body = _create(client)
    assert body["row_count"] >= 1
    assert len(list(directory.glob("*.csv"))) == 1


def test_startup_reconcile_removes_stale_temp_and_orphans(
    client: TestClient, archive_env, pg_dsn: str
) -> None:
    manager, directory = archive_env
    admin = psycopg.connect(pg_dsn, autocommit=True)
    try:
        admin.execute("TRUNCATE reading_archives")
    finally:
        admin.close()
    manager._directory()  # 建目录
    stale_tmp = directory / ".part-deadbeef-x.tmp"
    stale_tmp.write_bytes(b"partial")
    orphan = directory / "2020-01-01-orphan.csv"
    orphan.write_bytes(b"orphan")
    fresh_tmp = directory / ".part-cafebabe-y.tmp"
    fresh_tmp.write_bytes(b"in-flight")
    fresh_orphan = directory / "2099-01-01-inflight.csv"
    fresh_orphan.write_bytes(b"in-flight")
    import os

    old = time.time() - 7200
    for path in (stale_tmp, orphan):
        os.utime(path, (old, old))

    body = _create(client)
    keep = {p.name for p in directory.glob("*.csv")}
    # 两个孤儿 + 刚登记的正式文件
    assert len(keep) == 3

    manager.reconcile_storage()
    remaining = sorted(p.name for p in directory.iterdir())
    # 宽限期内的临时/孤儿文件不动（滚动发布不误删），已登记文件保留
    assert remaining == sorted(
        [
            f"{DAY}-{body['archive_id']}.csv",
            fresh_tmp.name,
            fresh_orphan.name,
        ]
    )
    # 已登记的封存仍可下载
    assert _read_csv(_download(client, body["archive_id"]))[0] == [
        "meter_id", "read_time", "wh"
    ]
