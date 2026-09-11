# Neo4j Graph Model & Idempotency Specification

## 1. Schema Topology

The database name is `CSV_Graph_DB`. The graph model uses a two-level hierarchy linking `Dataset` parents to individual `Row` entities:

```
(:Dataset {
    id: STRING,            // Deterministic SHA-256 hash
    filename: STRING,      // Uploaded filename
    uploaded_at: STRING    // ISO-8601 UTC timestamp
})
      |
      |  [:HAS_ROW]
      v
(:Row {
    dataset_id: STRING,    // Deterministic parent SHA-256 hash
    row_index: INTEGER,    // 0-indexed row position
    ...dynamic CSV column properties (e.g. Name, Department, Salary, etc.)
})
```

---

## 2. Uniqueness Constraints & Indexes

To ensure lightning-fast lookups and enforce absolute idempotency:

```cypher
// 1. Dataset ID uniqueness constraint
CREATE CONSTRAINT dataset_id_unique IF NOT EXISTS
FOR (d:Dataset) REQUIRE d.id IS UNIQUE;

// 2. Composite row identity uniqueness constraint
CREATE CONSTRAINT row_identity_unique IF NOT EXISTS
FOR (r:Row) REQUIRE (r.dataset_id, r.row_index) IS UNIQUE;
```

---

## 3. Parameterized Ingestion Cypher Pattern

All row persistence executes via the following parameterized Cypher query:

```cypher
// Step 1: Merge parent Dataset node
MERGE (d:Dataset {id: $dataset_id})
ON CREATE SET
  d.filename = $filename,
  d.uploaded_at = $uploaded_at
ON MATCH SET
  d.last_accessed_at = $uploaded_at

// Step 2: Merge child Row node with composite key (dataset_id + row_index)
MERGE (r:Row {
  dataset_id: $dataset_id,
  row_index: $row_index
})
SET r += $columns

// Step 3: Merge relationship linking Dataset to Row
MERGE (d)-[:HAS_ROW]->(r)
```

---

## 4. Idempotency Proof

When the **exact same CSV** is uploaded multiple times:
1. `dataset_id` is deterministically computed via SHA-256 over normalized content, yielding identical IDs.
2. `MERGE (d:Dataset {id: $dataset_id})` finds the existing Dataset node and avoids creating a duplicate.
3. For every row `i`, `MERGE (r:Row {dataset_id: $dataset_id, row_index: i})` finds the existing node with matching composite key. The dynamic properties are updated in place via `SET r += $columns`.
4. `MERGE (d)-[:HAS_ROW]->(r)` finds the existing relationship and avoids duplicate edges.

**Mathematical Invariant**:
$$\text{Count}(\text{Dataset}_1) == \text{Count}(\text{Dataset}_2)$$
$$\text{Count}(\text{Row}_1) == \text{Count}(\text{Row}_2)$$
$$\text{Count}(\text{HAS\_ROW}_1) == \text{Count}(\text{HAS\_ROW}_2)$$

Zero duplicate nodes or relationships are created upon subsequent identical uploads.
