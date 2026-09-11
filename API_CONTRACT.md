# API Contract Specification

This document details all external and internal HTTP endpoints exposed by the API service.

---

## 1. `POST /ingest`

Uploads a CSV file for ingestion through the Kafka pipeline.

- **Method**: `POST`
- **Path**: `/ingest`
- **Content-Type**: `multipart/form-data`
- **Form Fields**:
  - `file`: CSV file binary payload.

### Success Response: HTTP 202 Accepted
```json
{
  "job_id": "9f1d2c67-a4b8-4d33-912f-876543210abc",
  "rows_received": 100,
  "status": "queued"
}
```

### Error Responses:
- **HTTP 400 Bad Request**:
  - Non-CSV file uploaded (e.g. `.txt`, `.json`, `.exe`).
  - Empty 0-byte file uploaded.
  - Malformed CSV structure that fails parsing.
```json
{
  "detail": "Invalid file type. Only CSV files are accepted."
}
```
- **HTTP 202 with 0 Rows**:
  - Header-only CSV file (valid CSV headers with 0 data rows).
```json
{
  "job_id": "...",
  "rows_received": 0,
  "status": "complete"
}
```

---

## 2. `GET /status`

Queries the real-time ingestion progress and state for a specific job.

- **Method**: `GET`
- **Path**: `/status`
- **Query Parameters**:
  - `job_id`: string (UUID) - Required

### Success Response: HTTP 200 OK
```json
{
  "job_id": "9f1d2c67-a4b8-4d33-912f-876543210abc",
  "status": "loading",
  "rows_total": 100,
  "rows_loaded": 65,
  "rows_failed": 0
}
```

### Allowed States:
1. `queued`: Job created, Kafka messages published, loader pending consumption.
2. `loading`: Loader actively processing and persisting rows to Neo4j.
3. `complete`: All rows processed. **Mandatory condition**: `rows_loaded + rows_failed == rows_total`.
4. `failed`: Unrecoverable job failure.

### Error Response:
- **HTTP 404 Not Found**: Unknown `job_id`.
```json
{
  "detail": "Job not found"
}
```

---

## 3. `GET /health`

Performs active readiness checks on Kafka and Neo4j dependencies.

- **Method**: `GET`
- **Path**: `/health`

### Success Response: HTTP 200 OK (Both live)
```json
{
  "status": "ok",
  "kafka_connected": true,
  "neo4j_connected": true
}
```

### Unhealthy Response: HTTP 503 Service Unavailable (Either down)
```json
{
  "status": "not_ok",
  "kafka_connected": false,
  "neo4j_connected": true
}
```
*Note: `status` is "ok" ONLY when BOTH `kafka_connected` AND `neo4j_connected` are true.*

---

## 4. `POST /chat`

Submits natural language queries to be answered strictly against the Neo4j knowledge graph.

- **Method**: `POST`
- **Path**: `/chat`
- **Content-Type**: `application/json`
- **Request Body**:
```json
{
  "question": "How many employees are in HR?"
}
```

### Success Response: HTTP 200 OK (Grounded Query)
```json
{
  "answer": "There are 4 employees in the HR department.",
  "cypher": "MATCH (r:Row) WHERE toLower(toString(r.Department)) = 'hr' RETURN count(r) AS count",
  "result": [
    {
      "count": 4
    }
  ],
  "grounded": true
}
```

### Success Response: HTTP 200 OK (Ungrounded / Unsupported Query)
```json
{
  "answer": "I do not have data in the knowledge graph to answer this question.",
  "cypher": "NONE",
  "result": [],
  "grounded": false
}
```

---

## 5. `POST /internal/progress`

Internal endpoint used by the Loader service to communicate row persistence metrics to the API Job Tracker.

- **Method**: `POST`
- **Path**: `/internal/progress`
- **Content-Type**: `application/json`
- **Request Body**:
```json
{
  "job_id": "9f1d2c67-a4b8-4d33-912f-876543210abc",
  "loaded_delta": 10,
  "failed_delta": 0,
  "is_final": false
}
```
- **Response**: HTTP 200 OK `{"status": "updated"}`
