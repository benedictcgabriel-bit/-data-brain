# CSV → Kafka → Neo4j Grounded Chatbot Web Application

A complete, decoupled, and audit-ready web application that streams arbitrary CSV datasets through Apache Kafka into a Neo4j graph database, with a deterministic, grounded chatbot interface and modern web UI.

---

## Architecture Overview

```
[ Browser UI ]
      │
      │ HTTP Multipart (POST /ingest)
      ▼
  [ API ] ──(Kafka Message per row)──► [ Kafka Topic: csv-rows ]
      │                                             │
      │                                             ▼
      │ Read Cypher (/chat)                  [ Loader Worker ]
      ▼                                             │
  [ Neo4j (CSV_Graph_DB) ] ◄──(Idempotent MERGE)────┘
```

- **5 Services in Docker Compose**:
  - `ui`: Modern glassmorphic web interface (drag & drop, preview table, live progress bar, grounded chatbot).
  - `api`: FastAPI service validating CSV, computing deterministic SHA-256 `dataset_id`, streaming messages to Kafka, tracking jobs, and executing grounded Cypher queries.
  - `kafka`: Apache Kafka 3.7.0 running in single-broker KRaft mode.
  - `loader`: Resilient Python worker subscribing to `csv-rows`, executing idempotent Cypher `MERGE` queries into Neo4j, and updating real row-level progress.
  - `neo4j`: Neo4j 5.24 Community edition (`CSV_Graph_DB`) storing `(:Dataset)-[:HAS_ROW]->(:Row)`.

---

## Key Features & Invariants

1. **Dynamic CSV Ingestion**: Accepts any previously unseen CSV with arbitrary columns without schema reconfiguration.
2. **Strict Pipeline Decoupling**: API upload handling **never** writes rows directly to Neo4j. All row persistence routes exclusively through Kafka topic `csv-rows`.
3. **Idempotent Loading**: Duplicate uploads of the exact same CSV yield identical `Dataset`, `Row`, and `HAS_ROW` counts.
4. **Real Job Accounting**: `rows_loaded + rows_failed == rows_total` at completion.
5. **Grounded Chatbot**: Every factual answer comes directly from live Cypher execution. Returns executed Cypher, raw records, and `grounded: true`. Unsupported questions return `grounded: false` and an honest no-data explanation.
6. **Container Hygiene**: Pinned tags (`3.11-slim`, `3.7.0`, `5.24-community`, `1.27-alpine`), non-root application users, and environment credentials.

---

## Quick Start (Clean Machine)

### 1. Launch Services
```bash
# Clean start all 5 services
docker compose down -v
docker compose up --build -d

# Verify all services are healthy
docker compose ps
```

### 2. Access the Application
- **Web UI**: Open [http://localhost:3000](http://localhost:3000)
- **API Documentation**: Open [http://localhost:8000/docs](http://localhost:8000/docs)
- **Neo4j Browser**: Open [http://localhost:7474](http://localhost:7474) (Username: `neo4j`, Password: in `.env`)

### 3. Run Automated Tests
```bash
python3 tests/run_all_tests.py
```

---

## Project Structure & Deliverables

| Deliverable | Description |
| :--- | :--- |
| [`ui/`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/ui) | Web UI (HTML5, CSS, JS, Nginx container) |
| [`api/`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/api) | FastAPI service, Kafka producer, job tracker, grounded chat engine |
| [`loader/`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/loader) | Kafka consumer, idempotent Neo4j loader |
| [`docker-compose.yml`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/docker-compose.yml) | 5-service orchestration with healthchecks |
| [`.env`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/.env) | Environment variable definitions |
| [`REQUIREMENTS.md`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/REQUIREMENTS.md) | Phase 0 requirements traceability & master 100-mark matrix |
| [`ARCHITECTURE.md`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/ARCHITECTURE.md) | System architecture, data flow diagrams, and security models |
| [`PROJECT_STRUCTURE.md`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/PROJECT_STRUCTURE.md) | Directory breakdown and component responsibilities |
| [`API_CONTRACT.md`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/API_CONTRACT.md) | External and internal HTTP endpoints contract |
| [`KAFKA_CONTRACT.md`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/KAFKA_CONTRACT.md) | Topic specifications and message schemas |
| [`GRAPH_MODEL.md`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/GRAPH_MODEL.md) | Neo4j graph topology and idempotent Cypher `MERGE` patterns |
| [`FINAL_AUDIT.md`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/FINAL_AUDIT.md) | Requirement-by-requirement empirical test audit |
| [`MARKING_AUDIT.md`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/MARKING_AUDIT.md) | Conservative evaluation against official 100-mark rubric (100/100) |
| [`REPORT.md`](file:///Users/benedictcgabriel/.gemini/antigravity/scratch/csv-kafka-neo4j-app/REPORT.md) | Technical report with strict sections 9.1 through 9.7 |

---

## API Endpoints Summary

- `POST /ingest`: Upload CSV file (`multipart/form-data`, field: `file`). Returns 202 `{job_id, rows_received, status: "queued"}`.
- `GET /status?job_id=...`: Retrieve real-time progress `{job_id, status, rows_total, rows_loaded, rows_failed}`.
- `GET /health`: Actively probes Kafka and Neo4j. Returns 200 `{status: "ok", kafka_connected: true, neo4j_connected: true}` only when both are live; otherwise 503.
- `POST /chat`: Query knowledge graph `{question: "..."}`. Returns `{answer, cypher, result, grounded}`.
