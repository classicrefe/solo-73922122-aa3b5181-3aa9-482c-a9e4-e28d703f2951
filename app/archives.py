"""读数封存：把指定 UTC 日期的原始读数写成可下载的 UTF-8 CSV。

一致性：
- 每条封存只使用一个已提交快照（REPEATABLE READ + pg_export_snapshot），
  封存期间其他会话对 readings 的更正不会进入本次文件；
- 再次封存会生成新的 archive_id / 文件，形成独立版本。

发布原子性：
1. 先写同目录下的临时文件（`.part`），全程流式、不复用内存中的整表；
2. 写完 flush + fsync，再原子 rename 为正式文件名，并 fsync 目录；
3. 最后才向 PostgreSQL 登记记录；
4. 写文件或登记任一步失败都会尽力删除本次产物，绝不留下
   “可下载的半份文件”或“指向缺失文件的成功记录”；
5. 下载只按数据库记录解析文件名，未登记的文件不可下载。
"""
from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import stat
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path

from . import db as dbmod
from .config import settings

# 封存编号只接受 32 位小写十六进制，避免任何路径拼接风险
_ARCHIVE_ID_RE = re.compile(r"^[0-9a-f]{32}$")

_CSV_HEADER = ("meter_id", "read_time", "wh")

# read_time 统一输出为 UTC ISO-8601（含微秒与 Z），不受会话时区影响
_ARCHIVE_SQL = """
SELECT meter_id::bigint AS meter_id,
       to_char(
           read_time AT TIME ZONE 'UTC',
           'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
       ) AS read_time,
       wh::bigint AS wh
FROM readings
WHERE read_time >= %s::timestamptz
  AND read_time <  %s::timestamptz
ORDER BY meter_id, read_time
"""

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS reading_archives (
    archive_id   text        PRIMARY KEY,
    archive_date date        NOT NULL,
    row_count    bigint      NOT NULL CHECK (row_count >= 0),
    file_sha256  text        NOT NULL CHECK (file_sha256 ~ '^[0-9a-f]{64}$'),
    file_name    text        NOT NULL,
    file_size    bigint      NOT NULL CHECK (file_size >= 0),
    created_at   timestamptz NOT NULL DEFAULT now()
)
"""

_SELECT_COLUMNS = (
    "archive_id, archive_date, row_count, file_sha256, "
    "file_name, file_size, created_at"
)


class ArchiveError(RuntimeError):
    """封存流程中的非预期失败（写文件或登记数据库）。"""


class ArchiveNotFound(LookupError):
    """封存编号不存在。"""


class ArchiveFileMissing(RuntimeError):
    """记录存在但文件缺失或大小不符，不可作为成功结果对外提供。"""

    def __init__(self, archive_id: str):
        super().__init__(f"封存 {archive_id} 的文件缺失或已损坏")
        self.archive_id = archive_id


@dataclass(frozen=True)
class ArchiveRecord:
    archive_id: str
    archive_date: date
    row_count: int
    file_sha256: str
    file_name: str
    file_size: int
    created_at: datetime


def _utc_day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, dtime.min, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _fsync_directory(path: str) -> None:
    """确保 rename 在掉电/重启后仍可见（Linux 上对目录做 fsync）。"""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _quiet_unlink(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


class ArchiveManager:
    def __init__(self) -> None:
        self._schema_lock = threading.Lock()
        self._schema_ready_dsn: str | None = None
        self._dir_lock = threading.Lock()
        self._dir_ready: str | None = None

    # ---------- 存储 / 元数据基础 ----------
    def _directory(self) -> str:
        """返回封存目录，按当前配置动态解析（便于测试覆盖）。"""
        directory = str(Path(settings.archive_dir))
        with self._dir_lock:
            if self._dir_ready != directory or not os.path.isdir(directory):
                try:
                    os.makedirs(directory, exist_ok=True)
                except OSError as exc:
                    raise ArchiveError(f"封存目录不可用：{directory}：{exc}") from exc
                self._dir_ready = directory
        return directory

    def _meta_connection(self):
        return dbmod.open_connection(application_name="recon-api-archive-meta")

    def ensure_schema(self) -> None:
        """幂等建表；进程内只做一次，兼容已初始化过旧库的环境。"""
        dsn = settings.pg_dsn
        with self._schema_lock:
            if self._schema_ready_dsn == dsn:
                return
            conn = self._meta_connection()
            try:
                try:
                    conn.execute(_CREATE_TABLE_SQL)
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_reading_archives_date "
                        "ON reading_archives (archive_date, created_at DESC)"
                    )
                except Exception:
                    # 多进程同时首次建表可能撞车；只要表已存在即可继续
                    exists = conn.execute(
                        "SELECT to_regclass('public.reading_archives') IS NOT NULL"
                    ).fetchone()
                    if not (exists and exists[0]):
                        raise
            finally:
                conn.close()
            self._schema_ready_dsn = dsn

    @staticmethod
    def _row_to_record(row: tuple) -> ArchiveRecord:
        return ArchiveRecord(
            archive_id=row[0],
            archive_date=row[1],
            row_count=row[2],
            file_sha256=row[3],
            file_name=row[4],
            file_size=row[5],
            created_at=row[6],
        )

    def _insert_record(
        self,
        *,
        archive_id: str,
        archive_date: date,
        row_count: int,
        file_sha256: str,
        file_name: str,
        file_size: int,
    ) -> ArchiveRecord:
        conn = self._meta_connection()
        try:
            row = conn.execute(
                "INSERT INTO reading_archives "
                "(archive_id, archive_date, row_count, file_sha256, "
                " file_name, file_size) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                f"RETURNING {_SELECT_COLUMNS}",
                (
                    archive_id,
                    archive_date,
                    row_count,
                    file_sha256,
                    file_name,
                    file_size,
                ),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise ArchiveError("封存记录登记失败：数据库未返回已登记记录")
        return self._row_to_record(row)

    # ---------- 创建封存 ----------
    def create(self, archive_date: date) -> ArchiveRecord:
        """封存指定 UTC 日期的原始读数；成功后文件与记录同时可用。"""
        self.ensure_schema()
        directory = self._directory()
        archive_id = uuid.uuid4().hex
        file_name = f"{archive_date.isoformat()}-{archive_id}.csv"
        final_path = os.path.join(directory, file_name)

        session = dbmod.SnapshotSession(
            reader_count=1,
            statement_timeout_ms=settings.archive_statement_timeout_ms,
        )
        tmp_path: str | None = None
        try:
            session.open()
            fd, tmp_path = tempfile.mkstemp(
                prefix=f".part-{archive_id[:8]}-", suffix=".tmp", dir=directory
            )
            row_count, file_sha256, file_size = self._stream_to_temp(
                session.readers[0], archive_date, fd
            )
            os.replace(tmp_path, final_path)  # 同目录原子发布
            tmp_path = None
            _fsync_directory(directory)
        except Exception as exc:
            if tmp_path is not None:
                _quiet_unlink(tmp_path)
            if isinstance(exc, (dbmod.DbError, ArchiveError)):
                raise
            raise ArchiveError(f"封存文件写入失败：{exc}") from exc
        finally:
            session.close()

        record_kwargs = dict(
            archive_id=archive_id,
            archive_date=archive_date,
            row_count=row_count,
            file_sha256=file_sha256,
            file_name=file_name,
            file_size=file_size,
        )
        try:
            return self._insert_record(**record_kwargs)
        except Exception as exc:
            # 登记失败：撤掉已发布的文件（含 rename 后、fsync 前崩溃的情形，
            # 启动时 reconcile_storage 也会按“文件无记录”兜底清理）。
            _quiet_unlink(final_path)
            if isinstance(exc, ArchiveError):
                raise
            raise ArchiveError(f"封存记录登记失败：{exc}") from exc

    def _stream_to_temp(self, conn, archive_date: date, fd: int) -> tuple[int, str, int]:
        """在调用方给定的快照连接上流式写 CSV，返回 (行数, sha256, 字节数)。

        fd 的所有权在本方法内：无论成功失败都会关闭。
        """
        start, end = _utc_day_bounds(archive_date)
        cursor_name = f"arc_{uuid.uuid4().hex[:16]}"
        cur = conn.cursor(name=cursor_name, binary=False, withhold=False)
        cur.itersize = max(1000, min(settings.chunk_rows, 10000))
        raw = os.fdopen(fd, "wb")
        hasher = hashlib.sha256()
        size = 0
        row_count = 0
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")

        def flush_buffer() -> None:
            nonlocal size
            data = buf.getvalue().encode("utf-8")
            if not data:
                return
            raw.write(data)
            hasher.update(data)
            size += len(data)
            buf.seek(0)
            buf.truncate(0)

        try:
            cur.execute(_ARCHIVE_SQL, (start, end))
            if cur.description is None:
                raise ArchiveError("封存查询没有返回结果集")
            writer.writerow(_CSV_HEADER)
            while True:
                batch = cur.fetchmany(cur.itersize)
                if not batch:
                    break
                for row in batch:
                    writer.writerow(row)
                    row_count += 1
                flush_buffer()
            flush_buffer()
            raw.flush()
            os.fsync(raw.fileno())
        finally:
            try:
                cur.close()
            except Exception:
                pass
            raw.close()

        return row_count, hasher.hexdigest(), size

    # ---------- 查询 ----------
    def list(self, archive_date: date | None = None) -> list[ArchiveRecord]:
        self.ensure_schema()
        conn = self._meta_connection()
        try:
            if archive_date is None:
                rows = conn.execute(
                    f"SELECT {_SELECT_COLUMNS} FROM reading_archives "
                    "ORDER BY created_at DESC, archive_id"
                ).fetchall()
            else:
                rows = conn.execute(
                    f"SELECT {_SELECT_COLUMNS} FROM reading_archives "
                    "WHERE archive_date = %s "
                    "ORDER BY created_at DESC, archive_id",
                    (archive_date,),
                ).fetchall()
        finally:
            conn.close()
        return [self._row_to_record(row) for row in rows]

    def get(self, archive_id: str) -> ArchiveRecord | None:
        if not _ARCHIVE_ID_RE.match(archive_id):
            return None
        self.ensure_schema()
        conn = self._meta_connection()
        try:
            row = conn.execute(
                f"SELECT {_SELECT_COLUMNS} FROM reading_archives "
                "WHERE archive_id = %s",
                (archive_id,),
            ).fetchone()
        finally:
            conn.close()
        return self._row_to_record(row) if row is not None else None

    def resolve_download(self, archive_id: str) -> tuple[ArchiveRecord, str]:
        """解析可下载文件；记录不存在或文件缺失/大小不符时拒绝。"""
        record = self.get(archive_id)
        if record is None:
            raise ArchiveNotFound(archive_id)
        # file_name 入库前由本服务生成；仍做一次 basename 防御
        if os.path.basename(record.file_name) != record.file_name:
            raise ArchiveFileMissing(archive_id)
        path = os.path.join(self._directory(), record.file_name)
        try:
            st = os.stat(path)
        except OSError as exc:
            raise ArchiveFileMissing(archive_id) from exc
        if not stat.S_ISREG(st.st_mode) or st.st_size != record.file_size:
            raise ArchiveFileMissing(archive_id)
        return record, path

    # ---------- 启动自愈 ----------
    # 未登记产物的宽限期：避免滚动发布时新实例误删旧实例正在发布中的文件
    ORPHAN_GRACE_SECONDS = 3600

    def reconcile_storage(self) -> None:
        """清理上次中断留下的临时文件，以及长时间未被登记的孤儿正式文件。

        只处理超过宽限期、且数据库中确实没有对应记录的产物：
        - 临时文件（.part-*.tmp）从不会对外提供，超过宽限期即可删；
        - 正式 .csv 只有在没有登记记录时才删，已登记文件绝不动。
        宽限期用于兼容滚动发布（旧实例可能仍在写同卷）。
        """
        directory = self._directory()
        now = time.time()
        grace = self.ORPHAN_GRACE_SECONDS

        def old_enough(path: str) -> bool:
            try:
                return now - os.stat(path).st_mtime > grace
            except OSError:
                return False

        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if entry.name.startswith(".part-") and entry.name.endswith(".tmp"):
                        if old_enough(entry.path):
                            _quiet_unlink(entry.path)
        except OSError:
            return

        try:
            known = {rec.file_name for rec in self.list()}
        except Exception:
            # 数据库暂不可用时不猜测孤儿，留待下一次启动
            return
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if not entry.name.endswith(".csv") or entry.name in known:
                        continue
                    if old_enough(entry.path):
                        _quiet_unlink(entry.path)
        except OSError:
            pass


# 模块级单例（配置在调用时动态读取）
archives = ArchiveManager()
