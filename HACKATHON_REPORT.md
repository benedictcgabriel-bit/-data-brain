# 🧠 DATA BRAIN: From CSV to Intelligent Answers
### Hackathon Project Submission & Technical Showcase

---

## 📌 Executive Summary

| Project Detail | Information |
| :--- | :--- |
| **Project Name** | **DATA BRAIN** |
| **Tagline** | *From CSV to Intelligent Answers* |
| **Category** | Artificial Intelligence / Knowledge Graphs / Data Intelligence |
| **Core Value** | Zero-hallucination conversational interface over arbitrary tabular data powered by dynamic graph queries |
| **Live Interface** | Interactive Web Application running at `http://localhost:8080` |
| **Tech Stack** | Neo4j Graph DB, Apache Kafka, FastAPI (Python), Vanilla JS, Tailwind CSS, Docker |

---

## 🎯 Problem Statement

Every day, organizations generate millions of CSV files containing critical business records—employee databases, financial ledgers, inventory rosters, and clinical trials. However:

1. **Non-Technical Access Barrier**: Querying raw tabular data requires SQL/Cypher proficiency, spreadsheet formula expertise, or manual filtering.
2. **LLM Hallucination Trap**: Feeding tabular data into naive Generative AI or standard vector RAG results in fabricated numbers, missed aggregations, invented departments, and untrustworthy answers.
3. **Schema Rigidity**: Traditional databases demand rigid schema definitions and migrations whenever a new file format arrives.
4. **Lack of Auditability**: In regulated industries (finance, healthcare, governance), an AI must be able to prove *how* it arrived at an answer.

---

## 💡 The Solution: DATA BRAIN

**DATA BRAIN** solves these challenges by transforming raw, arbitrary CSV files into an active, dynamic knowledge graph and providing a 100% grounded conversational interface.

Instead of guessing or predicting text probabilistically, **DATA BRAIN**:
- Ingests arbitrary CSV files asynchronously with dynamic column inference.
- Translates natural language questions into precise, deterministic graph queries.
- Guarantees **Zero Hallucination**: Every factual claim is verified against graph ground truth. If data is missing or out-of-scope, it transparently responds with an honest `NOT GROUNDED` status.

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│  Arbitrary CSV  │  ───> │  Apache Kafka   │  ───> │  Neo4j Graph DB │
│  (Upload & Ingest)│     │  (Stream Buffer)│       │  (Knowledge Base)│
└─────────────────┘       └─────────────────┘       └────────┬────────┘
                                                             │
                                                             ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│  User Answers   │  <─── │ Grounding Engine│  <─── │ Natural Language│
│ (100% Verifiable)│      │(Anti-Hallucinate│       │  User Question  │
└─────────────────┘       └─────────────────┘       └─────────────────┘
```

---

## ✨ Key Features & Innovation

### 1. Dynamic Schema Adaptation
- Upload **any** tabular `.csv` file regardless of headers, order, or data types.
- Automatic column type detection (numeric, categorical, strings, identifiers).
- Live preview showing the first 5 rows and total column/row counts before ingestion.

### 2. High-Performance Asynchronous Ingestion
- Real-time ingestion state tracking: `QUEUED` ➔ `LOADING` ➔ `COMPLETE`.
- Dynamic progress bar with live counters for total rows, rows loaded, and failed rows.
- Complete decoupling between ingestion streaming and analytical querying.

### 3. Grounded Conversational Intelligence
- **Multi-Property Queries**: Understands combined requests such as:
  > *"Give me the name, department and role of employee 4"*  
  Returns all requested attributes clearly and accurately.
- **Aggregations & Statistics**: Accurately computes averages, minimums, maximums, and sums (e.g. *"What is the average salary?"*, *"Highest price"*).
- **Entity & Group Counts**: Accurately counts entities (e.g. *"How many employees?"*, *"Total rows"*) without hallucination.
- **Categorical Listings**: Extracts distinct categories (e.g. *"List all departments"*).

### 4. Mathematical Anti-Hallucination Guarantee
- Every response carries an unambiguous verification badge:
  - `✓ GROUNDED (FROM GRAPH)` for verified answers from the knowledge graph.
  - `✗ NOT GROUNDED (UNSUPPORTED/NO DATA)` for queries outside the dataset (e.g., asking *"What is the capital of France?"* on an employee dataset).
- Eliminates AI overconfidence and ensures audit compliance.

### 5. Luminous, Modern Glassmorphic UI/UX
- **Intro Animation**: Branded letter-by-letter drop animation (*D-A-T-A &nbsp; B-R-A-I-N*) that smoothly rolls up like an elegant curtain to reveal the app.
- **Color Palette**: Soft pastel luminous background (`#F6F7FB`) with subtle warm peach and lavender radial mesh gradients.
- **Signature Violet Accent**: Primary controls and badges styled in vibrant violet (`#7C5CFC` to `#6842ED`).
- **Dedicated Workflow**: Clean tab navigation separating **Upload CSV** and **Chatbot** screens.
- **Dynamic Suggestion Chips**: Chips update automatically to reflect the specific columns and records of the uploaded file.

---

## 🏗 System Architecture & Technology Stack

| Layer | Component | Technologies Used | Purpose |
| :--- | :--- | :--- | :--- |
| **Frontend** | Single Page Application | HTML5, Tailwind CSS, ES6+ JavaScript | Glassmorphic UI, responsive layout, letter-drop animations |
| **API Gateway** | RESTful Microservice | FastAPI, Uvicorn, Pydantic | Ingestion endpoints, job tracking, query routing |
| **Message Streaming** | Distributed Log Buffer | Apache Kafka 7.4.0, ZooKeeper | Streaming row-by-row buffer (`csv-rows` topic) |
| **Loader Worker** | Ingestion Consumer | Python 3.11, Confluent-Kafka, Neo4j Driver | Consumes rows, performs idempotent graph inserts |
| **Knowledge Graph** | Primary Data Store | Neo4j 5.15 Community Edition | Entity-property graph storage, Cypher query execution |
| **Packaging** | Microservices Architecture | Docker, Docker Compose | Complete containerization of all 5 services |

---

## 🧪 Verification, Testing & Quality Assurance

The project includes an automated test suite verifying 24 critical assertions across 5 testing domains:

| Test Suite | Tests Run | Result | Coverage |
| :--- | :---: | :---: | :--- |
| `test_api_contracts.py` | 5 | **PASS** | Validates HTTP schema, UUID generation, multipart uploads |
| `test_grounded_chat.py` | 10 | **PASS** | Cypher translation, multi-property queries, aggregations, anti-hallucination |
| `test_idempotency.py` | 3 | **PASS** | Re-uploading identical rows updates in-place without duplicate nodes |
| `test_hostile_inputs.py` | 3 | **PASS** | Special characters, Cyrillic Unicode, SQL/Cypher injection defense |
| `test_health_readiness.py`| 3 | **PASS** | Readiness probes, connectivity checks, dependency degradation |
| **Total** | **24** | **100% PASS** | Zero regressions across all edge cases |

---

## 🚀 Step-by-Step User Journey / Demo Flow

1. **Brand Intro**:
   - Opening the link triggers the **DATA BRAIN** intro animation: letters drop down with smooth spring physics, followed by the screen sliding upward to unveil the app.
2. **Dataset Selection**:
   - In the **Upload CSV** tab, drag & drop or browse any `.csv` file.
   - Instantly view the table preview of the first 5 rows and column summary.
3. **Ingest & Process**:
   - Click **Upload & Ingest CSV**.
   - Watch the animated progress bar update from `QUEUED` to `LOADING` to `COMPLETE`.
4. **Switch to Chatbot**:
   - Click **Open Chatbot to Query This Data**.
   - Notice the dataset tag reflects the active filename and exact row count.
5. **Ask Complex Queries**:
   - Click one of the dynamic chips or type in the chat box:
     - *"Give me the name, department and role of employee 4"*
     - *"What is the average salary?"*
     - *"How many employees are there?"*
   - Each answer is returned with the `✓ GROUNDED (FROM GRAPH)` status badge.
6. **Anti-Hallucination Verification**:
   - Ask an out-of-dataset question (e.g. *"What is the capital of France?"*).
   - The bot accurately responds with `✗ NOT GROUNDED (UNSUPPORTED/NO DATA)` with zero fabrication.

---

## 📈 Real-World Impact & Future Scope

### Target Applications:
- **Corporate HR & Operations**: Query employee directories, compensation bands, and team allocations without building custom dashboards.
- **Healthcare & Clinical Records**: Query patient cohorts, lab measurements, and treatment statuses with mathematical proof of answers.
- **Financial Compliance**: Audit transactions, accounts, and risk balances with guaranteed traceability.

### Future Roadmap:
- **Multi-Table Joins**: Automated graph relationship extraction across multiple related CSV files (e.g., `orders.csv` linked to `customers.csv`).
- **Voice Query Support**: Speech-to-text integration for hands-free executive querying.
- **Vector-Graph Hybrid Search**: Combining lexical keyword search, Cypher graph traversal, and embedding similarity for unstructured text columns.

---

## 👥 Team & Acknowledgments

- **Application Architect & Backend Engineer**: Architecture design, Kafka streaming pipeline, Neo4j graph model.
- **AI & Grounding Engineer**: Deterministic Cypher query engine, anti-hallucination validation, test suite.
- **Frontend & UI/UX Designer**: Glassmorphic theme, responsive mobile-friendly layout, letter-drop animations.

---
*Created for Hackathon Presentation — **DATA BRAIN**: Transforming raw tabular data into verified, intelligent answers.*
