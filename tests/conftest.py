"""pytest 夹具：优先使用环境变量 PG_DSN 指定的库；
否则尝试用 pgserver 拉起一个嵌入式 PostgreSQL（仅本地自测用）。"""
from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

INIT_SQL = Path(__file__).resolve().parents[1] / "sql" / "init.sql"


def _can_connect(dsn: str) -> bool:
    import psycopg

    try:
        conn = psycopg.connect(dsn, connect_timeout=3)
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    dsn = os.environ.get("PG_DSN")
    if dsn and _can_connect(dsn):
        return dsn

    # 嵌入式 postgres（pgserver 把数据目录的 unix socket 作为 host）
    data_dir = Path(os.environ.get("PG_EMBEDDED_DIR", "/tmp/pgdata"))
    data_dir.mkdir(parents=True, exist_ok=True)
    import pgserver

    # 保持引用以避免嵌入式服务器对象被回收（沙箱内不主动 stop）
    pgserver.get_server(str(data_dir))
    candidate = f"postgresql://postgres@/postgres?host={data_dir}"
    deadline = time.time() + 20
    while time.time() < deadline:
        if _can_connect(candidate):
            return candidate
        time.sleep(0.5)
    pytest.skip("嵌入式 PostgreSQL 未就绪")


@pytest.fixture(scope="session", autouse=True)
def _init_schema(pg_dsn: str) -> None:
    import psycopg

    conn = psycopg.connect(pg_dsn, autocommit=True)
    try:
        conn.execute(INIT_SQL.read_text(encoding="utf-8"))
        # 封存记录跨用例共享同一数据库：每个用例从空表开始，互不影响
        conn.execute("TRUNCATE reading_archives")
    finally:
        conn.close()


@pytest.fixture()
def client(pg_dsn, monkeypatch, tmp_path):
    """每个测试拿到干净的 JobManager 与 TestClient。"""
    archive_dir = tmp_path / "archives"
    monkeypatch.setenv("PG_DSN", pg_dsn)
    monkeypatch.setenv("WORK_DIR", "/tmp/recon-test-work")
    monkeypatch.setenv("ARCHIVE_DIR", str(archive_dir))

    from app import config as config_mod
    from app import manager as manager_mod
    from app.main import app

    # settings 在导入时定型，这里直接改运行期单例字段
    monkeypatch.setattr(config_mod.settings, "pg_dsn", pg_dsn)
    monkeypatch.setattr(config_mod.settings, "work_dir", "/tmp/recon-test-work")
    monkeypatch.setattr(config_mod.settings, "archive_dir", str(archive_dir))

    # 替换为全新管理器，保证测试间互不占用名额
    fresh = manager_mod.JobManager()
    monkeypatch.setattr(manager_mod, "manager", fresh)
    monkeypatch.setattr("app.main.manager", fresh)

    from fastapi.testclient import TestClient

    with TestClient(app) as c:
        yield c


@pytest.fixture()
def archive_env(pg_dsn, monkeypatch, tmp_path):
    """独立的封存目录与全新元数据缓存，确保每个用例互不干扰。"""
    archive_dir = tmp_path / "archives"
    monkeypatch.setattr(config_settings(), "pg_dsn", pg_dsn)
    monkeypatch.setattr(config_settings(), "archive_dir", str(archive_dir))

    from app import archives as archives_mod

    fresh = archives_mod.ArchiveManager()
    monkeypatch.setattr(archives_mod, "archives", fresh)
    monkeypatch.setattr("app.main.archive_mod.archives", fresh, raising=False)
    return fresh, archive_dir


def config_settings():
    from app import config as config_mod

    return config_mod.settings
