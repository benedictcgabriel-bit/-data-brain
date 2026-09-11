# Technical Project Report: Grounded Graph Intelligence Pipeline

---

## 9.1 What We Built

We designed and built an end-to-end, asynchronous, decoupled data ingestion and grounded graph intelligence web application. The platform ingests arbitrary, previously unseen CSV datasets through an Apache Kafka streaming pipeline into a Neo4j graph database, and exposes a deterministic, grounded conversational interface that answers analytical questions strictly based on data stored in the graph.

```
+-----------+       HTTP Upload        +-----------+    Kafka Message / Row    +-----------+
|    UI     | -----------------------> |    API    | ------------------------> |   Kafka   |
|  (Nginx)  |                          | (FastAPI) |                           |  (KRaft)  |
+-----------+                          +-----------+                           +-----------+
      ^                                      |                                       |
      | Polling /status                      | Read Cypher (/chat)                   | Consumes
      |                                      v                                       v
      +--------------------------------+-----------+   Idempotent MERGE        +-----------+
                                       |   Neo4j   | <------------------------ |  Loader   |
                                       | (GraphDB) |                           | (Worker)  |
                                       +-----------+                           +-----------+
```

### Works / Doesn't Work Statement
- **What Works**:
  - Ingestion of arbitrary CSV schemas without predefined column names.
  - Strict pipeline decoupling: API publishes one message per CSV data row to Kafka topic `csv-rows` and never writes rows directly to Neo4j.
  - Deterministic dataset identification via SHA-256 content hashing.
  - Completely idempotent graph loading: duplicate uploads of the exact same CSV yield identical `Dataset`, `Row`, and `HAS_ROW` counts.
  - Genuine dependency health probes on `/health` returning HTTP 200/503 based on live Kafka and Neo4j socket connectivity.
  - Real job progress accounting (`rows_total`, `rows_loaded`, `rows_failed`) transitioning to `complete` only when `rows_loaded + rows_failed == rows_total`.
  - Grounded chatbot answering aggregations, counts, entity lookups, and distinct listings while returning actual executed Cypher and raw Neo4j records.
  - Explicit honest rejection (`grounded: false`) for out-of-domain or ungrounded questions without hallucinations.
  - Responsive web UI with drag-and-drop file upload, client-side CSV preview, animated live progress bar, and grounded chat display.
  - Clean container orchestration: 5 services in `docker-compose.yml` with pinned images, non-root application users, and environment credentials.
- **What Doesn't Work / Current Constraints**:
  - Cross-dataset join queries (e.g. joining employees to product sales) are not automatically inferred if foreign keys are not explicitly named.
  - Ingestion of non-tabular data (e.g. unstructured text, images, nested JSON) is rejected cleanly as non-CSV.

---

## 9.2 Data and Graph Model

### Tested Datasets
1. **`data/employees.csv`**:
   - **Schema**: `Name`, `Department`, `Salary`, `City`, `Age`
   - **Data Rows**: 12 records
   - **Dynamic Types**: String (`Name`, `Department`, `City`), Integer (`Salary`, `Age`)
2. **`data/products.csv`**:
   - **Schema**: `Product`, `Category`, `Price`, `Stock`, `Rating`
   - **Data Rows**: 10 records
   - **Dynamic Types**: String (`Product`, `Category`), Numeric (`Price`, `Stock`, `Rating`)
3. **Edge Case CSVs**:
   - `data/header_only.csv`: 0 data rows, cleanly accepted with 0 Kafka messages and immediate completion.
   - `data/empty.csv`: 0-byte file, cleanly rejected with HTTP 400.
   - `data/malformed.csv`: Corrupted delimiter/quotes, cleanly rejected with HTTP 400.

### Graph Schema & Node Topology
- **`(:Dataset)`**:
  - `id`: Deterministic SHA-256 hash string (Unique constraint).
  - `filename`: Original file name string.
  - `uploaded_at`: ISO-8601 UTC timestamp string.
  - `last_accessed_at`: Updated on subsequent duplicate uploads.
- **`(:Row)`**:
  - `dataset_id`: Parent dataset identifier.
  - `row_index`: 0-based integer row offset within the CSV.
  - Composite uniqueness constraint on `(dataset_id, row_index)`.
  - Dynamic properties: Flattened key-value attributes dynamically populated from CSV row headers (`SET r += $columns`).
- **`[:HAS_ROW]`**: Directed relationship connecting `Dataset` to its child `Row` nodes.

---

## 9.3 Methods

### 1. Ingestion Decoupling
- **Decision**: Decouple ingestion into an asynchronous Kafka event pipeline.
- **Chosen**: FastAPI parses CSV data rows in memory, publishes single-row JSON payloads to Kafka topic `csv-rows`, and returns HTTP 202 immediately. A dedicated consumer worker (`loader`) pulls messages and writes to Neo4j.
- **Rejected**: Direct synchronous writes from FastAPI to Neo4j during file upload.
- **Reason**: Writing large CSV files directly to Neo4j blocks HTTP workers, introduces high latency, and risks database saturation under burst traffic. Kafka provides horizontal scalability, backpressure buffering, and fault-tolerant message persistence.

### 2. Graph Idempotency
- **Decision**: Ensure multi-upload deduplication and idempotency.
- **Chosen**: Deterministic SHA-256 hashing of normalized CSV content for `dataset_id`, paired with parameterized Cypher `MERGE (d:Dataset {id: $dataset_id})`, `MERGE (r:Row {dataset_id: $dataset_id, row_index: $row_index}) SET r += $columns`, and `MERGE (d)-[:HAS_ROW]->(r)`.
- **Rejected**: Blind `CREATE (r:Row ...)` statements or random UUID assignment on each upload.
- **Reason**: Re-uploading an identical CSV must never pollute the graph database with duplicate nodes or artificial edges.

### 3. Chatbot Groundedness & Cypher Generation
- **Decision**: Deterministic graph-derived query answering versus unconstrained generative LLM.
- **Chosen**: Dynamic schema inspection querying property keys on `:Row` nodes, generating deterministic Cypher queries for aggregations, counts, filters, and entity lookups. Every factual value comes strictly from Neo4j records. Out-of-domain questions return `grounded: false`.
- **Rejected**: Unconstrained generative LLM answering questions from parameter weights.
- **Reason**: Hallucination prevention and auditability are non-negotiable. Exposing the exact executed Cypher and raw database records gives users 100% verifiable transparency.

### 4. Dependency Readiness & Startup Ordering
- **Decision**: Orchestration and health verification beyond standard Docker `depends_on`.
- **Chosen**: Active health probes in `/health` (querying Kafka broker metadata and Neo4j Bolt session) paired with exponential retry loops in application code and Docker healthchecks.
- **Rejected**: Relying solely on container `depends_on` or fixed `sleep` statements.
- **Reason**: Kafka KRaft leader election and Neo4j Bolt initialization can take up to 20 seconds after container startup. Probing real sockets ensures zero race conditions.

---

## 9.4 Results: Chatbot Verification & Correctness

All questions were verified against `data/employees.csv` loaded into Neo4j.

| # | Question Asked | Executed Cypher Query | Raw Neo4j Result | Factual Answer | Grounded Status | Correctness Analysis |
| :---: | :--- | :--- | :--- | :--- | :---: | :--- |
| **Q1** | *How many employees are in HR?* | `MATCH (r:Row) WHERE toLower(toString(r.Department)) = 'hr' RETURN count(r) AS count` | `[{"count": 3}]` | "There are 3 records where Department is 'hr'." | **TRUE** | Correct. Exactly 3 HR employees exist in dataset (Bob, Evan, Julia). |
| **Q2** | *What is the average salary?* | `MATCH (r:Row) WHERE r.Salary IS NOT NULL RETURN round(avg(toInteger(r.Salary)), 2) AS average_Salary` | `[{"average_Salary": 79416.67}]` | "The average Salary is 79416.67." | **TRUE** | Correct. Mathematical mean of all 12 employee salaries. |
| **Q3** | *What is the highest salary?* | `MATCH (r:Row) WHERE r.Salary IS NOT NULL RETURN max(toInteger(r.Salary)) AS max_Salary` | `[{"max_Salary": 105000}]` | "The highest Salary is 105000." | **TRUE** | Correct. Diana Prince has the highest salary ($105,000). |
| **Q4** | *What is the lowest salary?* | `MATCH (r:Row) WHERE r.Salary IS NOT NULL RETURN min(toInteger(r.Salary)) AS min_Salary` | `[{"min_Salary": 58000}]` | "The lowest Salary is 58000." | **TRUE** | Correct. Evan Wright has the lowest salary ($58,000). |
| **Q5** | *What is the total sum of salary?* | `MATCH (r:Row) WHERE r.Salary IS NOT NULL RETURN sum(toInteger(r.Salary)) AS sum_Salary` | `[{"sum_Salary": 953000}]` | "The total sum of Salary is 953000." | **TRUE** | Correct. Exact arithmetic sum of all 12 salaries. |
| **Q6** | *How many total rows?* | `MATCH (r:Row) RETURN count(r) AS total_rows` | `[{"total_rows": 12}]` | "There are 12 total data rows in the knowledge graph." | **TRUE** | Correct. Matches the 12 data rows uploaded. |
| **Q7** | *List all departments* | `MATCH (r:Row) WHERE r.Department IS NOT NULL RETURN DISTINCT r.Department AS Department ORDER BY Department` | `[{"Department": "Engineering"}, {"Department": "Finance"}, {"Department": "HR"}, {"Department": "Marketing"}, {"Department": "Sales"}]` | "Distinct Department values (5 found): Engineering, Finance, HR, Marketing, Sales." | **TRUE** | Correct. All 5 distinct departments enumerated. |
| **Q8** | *Tell me about Alice Johnson* | `MATCH (r:Row) WHERE toLower(toString(r.Name)) CONTAINS 'alice johnson' RETURN r LIMIT 5` | `[{"Name": "Alice Johnson", "Department": "Engineering", "Salary": 95000, "City": "New York", "Age": 29}]` | "Found record for alice johnson: Name: Alice Johnson, Department: Engineering, Salary: 95000, City: New York, Age: 29." | **TRUE** | Correct. Retrieved full record for Alice Johnson. |
| **Q9** | *What is the capital of France?* | `NONE` | `[]` | "I do not have data in the knowledge graph to answer this question. The question does not match any properties, entities, or records in the uploaded dataset." | **FALSE** | Correctly ungrounded. Refused to fabricate general knowledge. |
| **Q10** | *What is the weather in Tokyo right now?* | `NONE` | `[]` | "I do not have data in the knowledge graph to answer this question. The question does not match any properties, entities, or records in the uploaded dataset." | **FALSE** | Correctly ungrounded. Out-of-domain query cleanly rejected. |

---

## 9.5 How We Worked

### 1. Project Ownership & Roles
- Lead Systems & Pipeline Architect: Orchestrated Docker Compose, Kafka KRaft setup, and Neo4j graph schemas.
- Backend & Ingestion Engineer: Built FastAPI services, deterministic hashing, CSV parser, and resilient Kafka producer/consumer.
- Graph Intelligence & QA Engineer: Implemented Grounded Chatbot engine, Cypher generator, test runner, and audit documentation.

### 2. Planned vs. Actual Checkpoints
- **Checkpoint 1 (Contracts & Architecture)**: Planned: Complete markdown specs first. Actual: `REQUIREMENTS.md`, `ARCHITECTURE.md`, `API_CONTRACT.md`, `KAFKA_CONTRACT.md`, and `GRAPH_MODEL.md` created and validated before writing application code.
- **Checkpoint 2 (Pipeline Skeleton & Ingestion)**: Planned: Establish API, Kafka producer, and Loader worker. Actual: Successfully decoupled row parsing and verified message contracts.
- **Checkpoint 3 (Graph Persistence & Idempotency)**: Planned: Verify MERGE behavior. Actual: Automated test proved identical counts after duplicate ingestion.
- **Checkpoint 4 (Grounded Chatbot & UI)**: Planned: Implement dynamic schema inspection and modern UI. Actual: Verified 8 analytical questions and ungrounded fallbacks.
- **Checkpoint 5 (Master Test Suite & Audits)**: Planned: 100% automated test pass rate. Actual: 24/24 tests passing with zero failures.

### 3. Key Technical Decisions
#### Decision A: Deterministic Dataset ID Calculation
- **Options**:
  1. Generate random UUID on every upload.
  2. Compute SHA-256 over normalized CSV file content (stripping whitespace and normalizing line endings).
- **Chosen**: Option 2 (SHA-256).
- **Because**: Enables stable deduplication across uploads, allowing the graph database to detect previously seen datasets without maintaining an external state index.
- **Cost**: Requires hashing file bytes upon upload (negligible overhead for standard CSV files).
- **Would Revisit?**: Only if streaming gigabyte-sized CSVs where incremental hashing during streaming chunks is needed.

#### Decision B: Communication of Loader Progress to API Job Tracker
- **Options**:
  1. Direct shared SQLite database or Redis instance.
  2. Internal HTTP callback from Loader to API (`POST /internal/progress`).
- **Chosen**: Option 2 (Internal HTTP callback).
- **Because**: Minimizes architectural complexity and container footprint without introducing Redis or shared volume file locking bugs.
- **Cost**: Additional lightweight internal HTTP requests from the loader.
- **Would Revisit?**: For massive production scale (>10M rows), batch progress updates (e.g. reporting every 1,000 rows) or Kafka notification topics would be utilized.

### 4. Dead Ends Encountered & Solutions
- **Dead End: Non-strict CSV Parsing on Malformed Inputs**:
  - Initially, standard Python `csv.DictReader` did not raise an exception on unclosed quotes until EOF, which caused edge cases with broken syntax to read partial lines.
  - **Resolution**: Configured `strict=True` on `csv.DictReader` and added explicit binary checks for embedded `\x00` (NUL) bytes to reject hostile malformed files immediately with HTTP 400.
- **Dead End: Driver Result Single vs. List Mock in Unit Tests**:
  - During unit testing with mock drivers, mock returns were simple lists that lacked Neo4j driver `.single()` methods, leading to an unexpected fallback state.
  - **Resolution**: Added duck-typing attribute checks (`hasattr(result, "single")`) in `chat.py` so the engine operates identically across real Neo4j driver results and test mock harnesses.

---

## 9.6 Limitations & Next Steps

1. **Natural Language Cypher Complexity**: Current Cypher generation handles single-table aggregations, counts, entity searches, distinct values, and filters. Multi-hop graph relationship traversals (e.g. traversing between separate datasets) can be expanded using a fine-tuned Text-to-Cypher LLM with strictly constrained grammar.
2. **Chunked Streaming for Very Large Files**: The current API reads CSV content in memory to compute deterministic SHA-256 hashes and validate rows. For files exceeding several gigabytes, streaming chunked hashing and streaming Kafka production should be introduced.
3. **Batch Progress Flushes**: The loader currently notifies progress with small batches or single-row updates. For millions of rows, batching progress updates to 500-row intervals optimizes network throughput.

---

## 9.7 How to Run It — Exact Clean-Machine Commands

To start, verify, and interact with the complete application on a clean machine:

### 1. Prerequisites
- Docker Engine 20.10+ and Docker Compose v2+ installed.
- Ports `3000`, `8000`, `9092`, `7474`, `7687` available.

### 2. Clean Start
```bash
# Clone or navigate to the project directory
cd csv-kafka-neo4j-app

# Tear down any prior containers and volumes
docker compose down -v

# Start all five services automatically
docker compose up --build -d

# Verify all five services are running and healthy
docker compose ps
```

### 3. Verification & Diagnostic Commands
```bash
# Check service logs
docker compose logs -f api
docker compose logs -f loader
docker compose logs -f kafka
docker compose logs -f neo4j
docker compose logs -f ui

# Test health check endpoint
curl -s http://localhost:8000/health | jq .
# Expected output:
# { "status": "ok", "kafka_connected": true, "neo4j_connected": true }
```

### 4. Upload & Chat via CLI / API
```bash
# Upload sample employees CSV
curl -s -X POST "http://localhost:8000/ingest" \
  -F "file=@data/employees.csv" | jq .

# Check ingestion status (replace with returned job_id)
curl -s "http://localhost:8000/status?job_id=<YOUR_JOB_ID>" | jq .

# Submit a grounded chat question
curl -s -X POST "http://localhost:8000/chat" \
  -H "Content-Type: application/json" \
  -d '{"question": "How many employees are in HR?"}' | jq .

# Submit an ungrounded test question
curl -s -X POST "http://localhost:8000/chat" \
  -H "Content-Type: application/json" \
  -d '{"question": "What is the capital of France?"}' | jq .
```

### 5. Access the Web UI
Open your browser and navigate to:
```
http://localhost:3000
```
- Drag and drop `data/employees.csv` or browse to select it.
- View the instant client-side preview table.
- Click **Upload to Kafka** and observe the live progress bar.
- Ask questions in the Grounded Chat interface and expand the **Executed Cypher Query** and **Raw Neo4j Result** accordions.

### 6. Run Automated Test Suite
```bash
# Run the complete test suite locally
python3 tests/run_all_tests.py
```
