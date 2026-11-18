"""FastAPI 应用入口与 HTTP 路由。"""
from __future__ import annotations

import contextlib
import os
from datetime import date as DateType

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse

from . import archives as archive_mod
from . import db as dbmod
from .archives import ArchiveError, ArchiveFileMissing, ArchiveNotFound
from .manager import SlotBusyError, manager
from .models import ArchiveOut, CreateArchiveRequest, CreateJobRequest, JobOut
from .sqlvalidation import SqlValidationError


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    """启动自愈：清掉上次中断留下的临时文件与未登记的孤儿文件。

    绝不删除已登记的封存文件；数据库暂不可用时直接跳过，不影响启动。
    """
    try:
        archive_mod.archives.reconcile_storage()
    except Exception:
        pass
    yield


app = FastAPI(
    title="电表日结 SQL 核对与读数封存 API",
    description=(
        "提交两条只读 SELECT，基于同一已提交快照并行读取，"
        "按行内容与出现次数做有序多重集核对（忽略行序，NULL≠空串），"
        "差异最多展示 20 种。单轮 30 秒，全局同时只允许一个任务。\n\n"
        "封存：按 UTC 日期把 readings 原始读数写成 CSV（meter_id, read_time, wh），"
        "返回封存编号、行数与文件 SHA-256，可按编号下载；"
        "每次封存基于独立快照形成独立版本，源表更正不影响已封存版本。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.get("/health", tags=["运维"])
def health() -> dict:
    """存活探针；不触碰数据库。"""
    return {"status": "ok"}


@app.get("/jobs", response_model=list[JobOut], tags=["核对任务"])
def list_jobs() -> list[JobOut]:
    """列出近期任务（含终态），状态查询始终可用。"""
    return manager.list_jobs()


@app.post("/jobs", response_model=JobOut, status_code=201, tags=["核对任务"])
def create_job(payload: CreateJobRequest) -> JobOut:
    try:
        job = manager.create_job(payload.left_sql, payload.right_sql, payload.name)
    except SqlValidationError as exc:
        raise HTTPException(status_code=400, detail=f"SQL 校验失败：{exc}") from exc
    except SlotBusyError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "当前已有核对任务在运行，一次只允许一组",
                "active_job_id": exc.active_job_id,
            },
        ) from exc
    return job.snapshot()


@app.get("/jobs/{job_id}", response_model=JobOut, tags=["核对任务"])
def get_job(job_id: str) -> JobOut:
    job = manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return job.snapshot()


@app.post("/jobs/{job_id}/cancel", response_model=JobOut, tags=["核对任务"])
def cancel_job(job_id: str) -> JobOut:
    try:
        job = manager.request_cancel(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc
    return job.snapshot()


@app.exception_handler(dbmod.DbError)
def _db_error_handler(_request, exc: dbmod.DbError) -> JSONResponse:
    return JSONResponse(status_code=422, detail={"message": str(exc)})


# ---------- 读数封存 ----------
@app.post(
    "/archives",
    response_model=ArchiveOut,
    status_code=201,
    tags=["读数封存"],
    summary="按 UTC 日期封存原始读数",
)
def create_archive(payload: CreateArchiveRequest) -> ArchiveOut:
    """从独立快照流式写出 CSV 并登记，返回封存编号、行数与 SHA-256。

    写入与登记是“先原子发布文件、后登记记录”；任一步失败都不留下
    可下载的半份文件或指向缺失文件的成功记录，可直接重试。
    """
    try:
        record = archive_mod.archives.create(payload.date)
    except ArchiveError as exc:
        raise HTTPException(status_code=503, detail={"message": str(exc)}) from exc
    except dbmod.DbError as exc:
        raise HTTPException(status_code=503, detail={"message": f"数据库不可用：{exc}"}) from exc
    return ArchiveOut.from_record(record)


@app.get(
    "/archives",
    response_model=list[ArchiveOut],
    tags=["读数封存"],
    summary="列出封存版本",
)
def list_archives(date: str | None = None) -> list[ArchiveOut]:
    """按创建时间倒序列出封存记录；可用 ?date=YYYY-MM-DD 过滤。"""
    parsed: DateType | None = None
    if date is not None:
        try:
            parsed = DateType.fromisoformat(date)
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail="date 需为 YYYY-MM-DD"
            ) from exc
    try:
        records = archive_mod.archives.list(parsed)
    except dbmod.DbError as exc:
        raise HTTPException(status_code=503, detail={"message": f"数据库不可用：{exc}"}) from exc
    return [ArchiveOut.from_record(r) for r in records]


@app.get(
    "/archives/{archive_id}",
    response_model=ArchiveOut,
    tags=["读数封存"],
    summary="查询单次封存",
)
def get_archive(archive_id: str) -> ArchiveOut:
    try:
        record = archive_mod.archives.get(archive_id)
    except dbmod.DbError as exc:
        raise HTTPException(status_code=503, detail={"message": f"数据库不可用：{exc}"}) from exc
    if record is None:
        raise HTTPException(status_code=404, detail="封存编号不存在")
    return ArchiveOut.from_record(record)


@app.get(
    "/archives/{archive_id}/download",
    tags=["读数封存"],
    summary="按编号下载封存 CSV",
    response_class=FileResponse,
    responses={
        200: {
            "description": "封存 CSV（UTF-8，表头 meter_id,read_time,wh）",
            "content": {
                "text/csv": {"schema": {"type": "string", "format": "binary"}}
            },
        },
        404: {"description": "封存编号不存在"},
        410: {"description": "记录存在但文件缺失或已损坏，请重新封存"},
    },
)
def download_archive(archive_id: str) -> FileResponse:
    """下载原始 CSV；记录存在但文件缺失/损坏时返回 410，不返回半份内容。"""
    try:
        record, path = archive_mod.archives.resolve_download(archive_id)
    except ArchiveNotFound as exc:
        raise HTTPException(status_code=404, detail="封存编号不存在") from exc
    except ArchiveFileMissing as exc:
        raise HTTPException(
            status_code=410,
            detail="封存文件缺失或已损坏，请重新封存该日期",
        ) from exc
    except dbmod.DbError as exc:
        raise HTTPException(status_code=503, detail={"message": f"数据库不可用：{exc}"}) from exc
    return FileResponse(
        path,
        media_type="text/csv; charset=utf-8",
        filename=os.path.basename(record.file_name),
        headers={
            "X-Archive-Id": record.archive_id,
            "X-Archive-Row-Count": str(record.row_count),
            "X-Archive-SHA256": record.file_sha256,
            "Cache-Control": "no-store",
        },
    )
