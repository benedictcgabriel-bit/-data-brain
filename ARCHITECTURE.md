# System Architecture Specification

## 1. High-Level Architecture Overview

The system is a distributed, decoupled data ingestion and grounded graph intelligence pipeline consisting of 5 independent containerized services:

```
+-----------------------------------------------------------------------------+
|                                Browser UI                                   |
|   (HTML5 / Tailwind CSS / Vanilla JS - Served by Nginx non-root container)  |
+------------------------------------+----------------------------------------+
                                     |
              HTTP Multipart Upload  |  HTTP Polling /status
              POST /ingest           |  GET /status?job_id=...
              POST /chat             |  GET /health
                                     v
+------------------------------------+----------------------------------------+
|                               API Service                                   |
|                (FastAPI / Python 3.11-slim non-root container)               |
|                                                                             |
|  - CSV Parser & Validator                                                  |
|  - Deterministic Dataset ID Generator (SHA-256)                             |
|  - Kafka Producer (publishes to 'csv-rows')                                 |
|  - Job Tracker & State Store (queued -> loading -> complete | failed)       |
|  - Grounded Chat Engine (Cypher Generator & Inspector)                      |
|  - Live Health Probes (Kafka AdminClient + Neo4j Bolt Session)              |
+-------------------+------------------------------------+--------------------+
                    |                                    |
     Publishes Row  |                                    | Read-Only Cypher
     Messages       |                                    | for /chat
                    v                                    v
+-------------------+--------------------+  +------------+--------------------+
|            Kafka Broker                |  |           Neo4j 5.24            |
|       (apache/kafka:3.7.0)             |  |      (Community Edition)        |
|                                        |  |                                 |
|  - Single Broker KRaft Mode            |  |  Database: CSV_Graph_DB         |
|  - Topic: 'csv-rows'                   |  |  Graph: (:Dataset)-[:HAS_ROW]-> |
|  - One message per data row            |  |         (:Row {dynamic props})  |
+-------------------+--------------------+  +------------+--------------------+
                    |                                    ^
                    | Consumes Row Messages              | Writes Persisted Rows
                    v                                    | via Idempotent MERGE
+-------------------+------------------------------------+--------------------+
|                              Loader Worker                                  |
|               (Python 3.11-slim non-root worker container)                  |
|                                                                             |
|  - Consumer loop with exponential backoff for Kafka leader election         |
|  - Neo4j Bolt connection pool with readiness retry loop                     |
|  - Batch / row idempotent MERGE execution                                   |
|  - Success / failure metric dispatch to Job Tracker                         |
+-----------------------------------------------------------------------------+
```

---

## 2. Ingestion Pipeline Decoupling Contract

### Critical Constraint
**The API service NEVER writes CSV rows directly to Neo4j.**
Row persistence is strictly decoupled through Kafka:
1. User uploads CSV to `POST /ingest`.
2. API validates headers and data rows. Header row is ignored for message publishing.
3. API computes deterministic `dataset_id = sha256(normalized_content)`.
4. API generates a unique `job_id` (UUID4) and records job metadata with status `queued`.
5. API dispatches exactly one Kafka message per data row to topic `csv-rows`.
6. API responds with HTTP 202 `{ "job_id": "...", "rows_received": N, "status": "queued" }`.
7. The `loader` service consumes messages from `csv-rows`, executes parameterized Cypher `MERGE` statements against Neo4j, and communicates actual persistent row progress to the API Job Tracker.

---

## 3. Grounded Chat Query Flow

1. User submits natural language question to `POST /chat` (e.g. *"How many employees are in HR?"*).
2. Chat engine inspects Neo4j schema dynamically:
   - Queries property keys on `:Row` nodes.
   - Extracts known attributes (e.g. `Department`, `Salary`, `Name`).
3. Query Formulation:
   - Identifies targets (counts, averages, filters, distinct values).
   - Generates deterministic Cypher query matching the dynamic properties.
4. Cypher Execution:
   - Executes Cypher query against `CSV_Graph_DB`.
   - Obtains raw query records.
5. Grounding & Response Verification:
   - If records are returned and answer is derived strictly from records:
     - Sets `grounded = true`.
     - Synthesizes factual answer.
     - Includes executed `cypher` and `result` array.
   - If query asks about non-existent attributes, unindexed entities, or unrelated domains (e.g. *"What is the speed of light?"*):
     - Sets `grounded = false`.
     - Sets `answer = "I do not have data in the knowledge graph to answer this question."`
     - Never hallucinates or uses external training weights.

---

## 4. Startup Ordering & Dependency Readiness

Because standard Docker `depends_on` only tracks container initialization rather than internal service availability:
- **Kafka KRaft Leader Election**: The broker may take several seconds to format cluster metadata, elect a controller, and accept client connections.
  - API and Loader use startup retry loops with exponential backoff (up to 60s) before attempting message dispatch or consumption.
- **Neo4j Bolt Socket Readiness**: The Bolt listener on port 7687 is only ready after database initialization and plugin loading.
  - Loader verifies Bolt connectivity via `RETURN 1` probe before initiating Kafka consumption.
- **Docker Compose Healthchecks**:
  - Kafka service uses `kafka-broker-api-versions.sh --bootstrap-server localhost:9092`.
  - Neo4j service uses `cypher-shell -u neo4j -p ${NEO4J_PASSWORD} -d CSV_Graph_DB 'RETURN 1'`.
  - API and Loader wait for `condition: service_healthy`.

---

## 5. Security & Container Hygiene

- **Pinned Image Tags**:
  - `apache/kafka:3.7.0` (Pinned, KRaft mode, no `:latest`).
  - `neo4j:5.24-community` (Pinned, Community edition, no `:latest`).
  - `python:3.11-slim` (Pinned base for API and Loader).
  - `nginx:1.27-alpine` (Pinned base for UI).
- **Non-Root Execution**:
  - API and Loader Dockerfiles create a dedicated group `appuser` (GID 10001) and user `appuser` (UID 10001).
  - Application processes execute under `USER 10001:10001`.
- **Credential Separation**:
  - Zero hardcoded passwords.
  - Environment variables loaded from `.env` (`NEO4J_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS`, etc.).
