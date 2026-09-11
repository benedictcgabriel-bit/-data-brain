# Kafka Contract Specification

## 1. Topic Definition

- **Topic Name**: `csv-rows`
- **Partitions**: 1 (Single partition guarantees ordered per-file ingestion in KRaft single broker mode)
- **Replication Factor**: 1
- **Cleanup Policy**: `delete`
- **Retention Period**: 86400000 ms (24 hours)

---

## 2. Message Payload Schema

Each Kafka message represents exactly ONE parsed CSV data row (header row excluded).

### JSON Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "CSVRowMessage",
  "type": "object",
  "required": [
    "job_id",
    "dataset_id",
    "filename",
    "row_index",
    "columns"
  ],
  "properties": {
    "job_id": {
      "type": "string",
      "format": "uuid",
      "description": "Unique identifier of the ingestion job triggering this message."
    },
    "dataset_id": {
      "type": "string",
      "description": "Deterministic SHA-256 hash derived from the CSV content for stable deduplication."
    },
    "filename": {
      "type": "string",
      "description": "Original file name uploaded by the client."
    },
    "row_index": {
      "type": "integer",
      "minimum": 0,
      "description": "0-based index of the data row within the CSV file."
    },
    "columns": {
      "type": "object",
      "description": "Key-value dictionary of column names to string/numeric values.",
      "additionalProperties": true
    }
  }
}
```

### Concrete Message Example
```json
{
  "job_id": "9f1d2c67-a4b8-4d33-912f-876543210abc",
  "dataset_id": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "filename": "employees.csv",
  "row_index": 0,
  "columns": {
    "Name": "Alice Johnson",
    "Department": "Engineering",
    "Salary": 95000,
    "City": "New York"
  }
}
```

---

## 3. Producer Guarantees
- Exactly one message produced per valid CSV data row.
- Zero messages produced for header lines or empty lines.
- Messages keyed by `dataset_id` to route rows of the same dataset to the same partition.
- `acks=all` (or `acks=1` in single-broker) ensuring persistence before HTTP 202 is returned.

---

## 4. Consumer Guarantees
- Consumer group: `csv-loader-group`.
- Offset commit: Committed after successful batch persistence to Neo4j.
- Exponential backoff retry on Neo4j connection drops.
- Failure isolation: If a single row fails type conversion or Cypher execution, it is recorded as `rows_failed += 1` and logged without stalling the consumer thread.
