import logging
import os
import uuid
from typing import Optional
from fastapi import FastAPI, File, HTTPException, Query, Response, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from chat import chat_engine
from job_tracker import tracker
from kafka_producer import kafka_service
from utils import parse_and_validate_csv

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("api.main")

app = FastAPI(
    title="CSV to Neo4j Grounded Chatbot API",
    version="1.0.0",
    description="Decoupled CSV ingestion via Kafka into Neo4j with grounded graph query capabilities.",
)

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    question: str


class ProgressUpdateRequest(BaseModel):
    job_id: str
    loaded_delta: int = 0
    failed_delta: int = 0
    mark_failed: bool = False


@app.get("/")
def root():
    return {
        "service": "CSV -> Kafka -> Neo4j Ingestion & Grounded Chatbot API",
        "endpoints": {
            "/ingest": "POST multipart/form-data (field: file)",
            "/status": "GET ?job_id=...",
            "/health": "GET",
            "/chat": "POST JSON {question: ...}",
        },
    }


@app.get("/health")
def get_health(response: Response):
    """
    Health check probe that genuinely tests reachability of Kafka and Neo4j.
    Only returns status: 'ok' if both downstream systems are genuinely reachable.
    """
    kafka_ok = kafka_service.check_health()
    neo4j_ok = chat_engine.check_health()

    is_overall_ok = kafka_ok and neo4j_ok
    if not is_overall_ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "status": "ok" if is_overall_ok else "not_ok",
        "kafka_connected": kafka_ok,
        "neo4j_connected": neo4j_ok,
    }


@app.post("/ingest", status_code=status.HTTP_202_ACCEPTED)
async def ingest_csv(file: UploadFile = File(...)):
    """
    Upload CSV file for asynchronous ingestion.
    Validates CSV, calculates deterministic dataset_id, and produces one
    Kafka message per data row to the 'csv-rows' topic.
    NEVER writes CSV rows directly to Neo4j.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename.")

    try:
        content = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read file: {e}")

    # Validate and parse CSV
    try:
        dataset_id, rows = parse_and_validate_csv(content, file.filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    job_id = str(uuid.uuid4())
    rows_received = len(rows)

    # If header-only (0 data rows), record job as complete immediately
    if rows_received == 0:
        tracker.create_job(
            job_id=job_id,
            dataset_id=dataset_id,
            filename=file.filename,
            rows_total=0,
        )
        return {
            "job_id": job_id,
            "rows_received": 0,
            "status": "complete",
        }

    # Record job in tracker with state 'queued'
    tracker.create_job(
        job_id=job_id,
        dataset_id=dataset_id,
        filename=file.filename,
        rows_total=rows_received,
    )

    # Publish one Kafka message per row
    try:
        for idx, row_data in enumerate(rows):
            message = {
                "job_id": job_id,
                "dataset_id": dataset_id,
                "filename": file.filename,
                "row_index": idx,
                "columns": row_data,
            }
            kafka_service.publish_row(message)
    except Exception as e:
        logger.error(f"Failed to publish row messages to Kafka: {e}")
        tracker.update_progress(job_id=job_id, mark_failed=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to publish messages to Kafka: {e}",
        )

    return {
        "job_id": job_id,
        "rows_received": rows_received,
        "status": "queued",
    }


@app.get("/status")
def get_job_status(job_id: str = Query(..., description="Unique job identifier")):
    """
    Retrieves real-time progress for an ingestion job.
    Allowed states: queued, loading, complete, failed.
    Complete only when rows_loaded + rows_failed == rows_total.
    """
    job_info = tracker.get_job(job_id)
    if not job_info:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job_info


@app.post("/chat")
def chat(request: ChatRequest):
    """
    Grounded Chat endpoint. Every factual answer comes strictly from Neo4j.
    Unsupported questions return grounded: false and an explicit no-data message.
    """
    if not request.question or not request.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")
    return chat_engine.process_query(request.question)


@app.post("/internal/progress")
def update_internal_progress(update: ProgressUpdateRequest):
    """
    Internal endpoint called by the Loader worker to report persisted row counts.
    """
    res = tracker.update_progress(
        job_id=update.job_id,
        loaded_delta=update.loaded_delta,
        failed_delta=update.failed_delta,
        mark_failed=update.mark_failed,
    )
    if not res:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {"status": "updated", "current": res}
