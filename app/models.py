"""API 请求 / 响应模型。"""
from __future__ import annotations

from datetime import date as DateType
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

JobStatus = Literal[
    "pending",      # 已受理，等待运行名额
    "running",      # 运行中
    "cancelling",   # 已收到取消请求，正在中止本轮 SQL
    "succeeded",    # 终态：核对完成（可能一致，也可能有差异）
    "failed",       # 终态：任一侧失败
    "cancelled",    # 终态：用户取消
    "timeout",      # 终态：超过单轮时限
]

TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "timeout"}
ACTIVE_STATUSES = {"pending", "running", "cancelling"}


class CreateJobRequest(BaseModel):
    left_sql: str = Field(..., min_length=1, description="左侧只读 SELECT（如改写后的日结 SQL）")
    right_sql: str = Field(..., min_length=1, description="右侧只读 SELECT（基准 SQL）")
    name: str | None = Field(None, max_length=128, description="可选备注名")


class DiffRow(BaseModel):
    # 与结果集列顺序对齐；NULL 用 null 表示，空串为 ""，二者不同
    values: list[Any]
    left_count: int
    right_count: int


class ReconcileResult(BaseModel):
    identical: bool
    columns: list[str]
    column_types: list[str]          # bigint | text
    left_row_total: int
    right_row_total: int
    matched_group_count: int         # 行内容与次数都相同的去重行数
    diff_group_total: int            # 存在差异的去重行种类总数（不受 20 条展示上限影响）
    diffs_shown: int
    diffs: list[DiffRow]             # 最多 20 种


class JobOut(BaseModel):
    job_id: str
    name: str | None
    status: JobStatus
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    deadline: datetime | None
    cancel_requested: bool
    error: str | None
    result: ReconcileResult | None

    @staticmethod
    def utcnow() -> datetime:
        return datetime.now(timezone.utc)


class CreateArchiveRequest(BaseModel):
    date: DateType = Field(
        ...,
        description="要封存的 UTC 日期，格式 YYYY-MM-DD（区间 [00:00, 次日 00:00) UTC）",
        examples=["2026-11-15"],
    )


class ArchiveOut(BaseModel):
    archive_id: str = Field(..., description="封存编号，用于下载")
    archive_date: DateType = Field(..., description="被封存的 UTC 日期")
    row_count: int = Field(..., description="CSV 数据行数（不含表头）")
    file_sha256: str = Field(..., description="CSV 文件 SHA-256（小写十六进制）")
    file_size: int = Field(..., description="CSV 文件字节数")
    created_at: datetime
    download_url: str = Field(..., description="按编号下载原文件的路径")

    @classmethod
    def from_record(cls, record) -> "ArchiveOut":
        return cls(
            archive_id=record.archive_id,
            archive_date=record.archive_date,
            row_count=record.row_count,
            file_sha256=record.file_sha256,
            file_size=record.file_size,
            created_at=record.created_at,
            download_url=f"/archives/{record.archive_id}/download",
        )
