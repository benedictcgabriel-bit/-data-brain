# Project Structure & Component Responsibilities

## 1. Directory Tree

```
csv-kafka-neo4j-app/
├── .env                              # Environment configuration (runtime secrets & connection strings)
├── .env.example                      # Template environment configuration for reproducible deployments
├── docker-compose.yml                # 5-service orchestration with healthchecks and dependencies
├── README.md                         # Quickstart guide, architectural summary, and operational instructions
│
├── api/                              # FastAPI Service (Port 8000)
│   ├── Dockerfile                    # Pinned python:3.11-slim, non-root user (UID 10001)
│   ├── requirements.txt              # FastAPI, uvicorn, kafka-python-ng, neo4j, python-multipart
│   ├── main.py                       # HTTP endpoints (/ingest, /status, /health, /chat, /internal/progress)
│   ├── job_tracker.py                # Thread-safe persistent job status state machine
│   ├── kafka_producer.py             # Resilient Kafka producer with retry & leader election handling
│   ├── chat.py                       # Grounded Cypher generation and execution engine
│   └── utils.py                      # Deterministic dataset hashing and CSV parsing utilities
│
├── loader/                           # Kafka -> Neo4j Consumer Service
│   ├── Dockerfile                    # Pinned python:3.11-slim, non-root user (UID 10001)
│   ├── requirements.txt              # kafka-python-ng, neo4j, requests, tenacity
│   └── consumer.py                   # Resilient consumer loop, idempotent MERGE, progress callbacks
│
├── ui/                               # Responsive Web Frontend (Port 3000)
│   ├── Dockerfile                    # Pinned nginx:1.27-alpine serving static assets
│   ├── nginx.conf                    # Nginx proxy pass configuration & static file hosting
│   ├── index.html                    # Single-page UI (Dropzone, CSV preview, Progress card, Chat panel)
│   ├── app.js                        # Client logic: file reader, API fetchers, polling, chat renderer
│   └── style.css                     # Modern clean CSS with glassmorphism, indicators, and dark-accented themes
│
├── tests/                            # Automated Verification Test Suite
│   ├── test_api_contracts.py         # Tests for /ingest, /status, /health, and schema handling
│   ├── test_hostile_inputs.py        # 11 hostile input scenarios (empty, header-only, malformed, non-CSV)
│   ├── test_idempotency.py           # Verification of identical graph counts after re-upload
│   ├── test_grounded_chat.py         # 8+ real questions and ungrounded rejection validation
│   ├── test_health_readiness.py      # Dependency failure and probe validation
│   └── run_all_tests.py              # Master test runner collecting empirical evidence for audits
│
├── data/                             # Sample Test Datasets
│   ├── employees.csv                 # Valid multi-column dataset
│   ├── products.csv                  # Valid dataset with completely different schema
│   ├── header_only.csv               # Edge case: header row with zero data rows
│   ├── empty.csv                     # Edge case: 0-byte file
│   └── malformed.csv                 # Edge case: corrupted CSV syntax
│
└── docs/                             # Core Specification & Audit Deliverables
    ├── REQUIREMENTS.md               # Master 100-mark requirements scorecard
    ├── ARCHITECTURE.md               # Architectural specification & sequence flows
    ├── PROJECT_STRUCTURE.md          # File directory map & component responsibilities (this file)
    ├── API_CONTRACT.md               # OpenAPI / HTTP contract documentation
    ├── KAFKA_CONTRACT.md             # Topic schema and producer/consumer guarantees
    ├── GRAPH_MODEL.md                # Neo4j schema, MERGE patterns, and constraints
    ├── FINAL_AUDIT.md                # Requirement-by-requirement audit with test evidence
    ├── MARKING_AUDIT.md              # 100-mark evaluation matrix with scores & justifications
    └── REPORT.md                     # Comprehensive technical report (Sections 9.1 - 9.7)
```

---

## 2. Component Responsibilities

| Component | Responsibility | Boundary Constraints |
| :--- | :--- | :--- |
| **`api`** | Validates incoming CSV uploads; computes deterministic SHA-256 dataset ID; streams row messages to Kafka; reports job status; hosts grounded chat engine; performs genuine health probes. | **NEVER writes CSV rows directly to Neo4j.** Strictly uses Kafka for data persistence. |
| **`loader`** | Subscribes to `csv-rows` on Kafka; formats dynamic properties; executes idempotent `MERGE` statements in Neo4j; communicates persistent row progress back to `api`. | Only consumes from Kafka. Operates idempotently across re-runs. |
| **`ui`** | Visual presentation: file dropzone, client-side CSV preview table, live progress bar, status metrics, interactive chat with Cypher inspection, raw result viewer, and grounded badges. | Calls API endpoints only. Never duplicates backend business logic. |
| **`kafka`** | High-throughput distributed log in KRaft mode. Buffers row-level events for decoupling API ingestion from graph database write speeds. | Single broker KRaft mode; topic `csv-rows`. |
| **`neo4j`** | Graph storage engine (`CSV_Graph_DB`). Stores `Dataset` and `Row` nodes linked by `HAS_ROW` relationships with dynamic properties. | Managed via parameterized Cypher statements. |
