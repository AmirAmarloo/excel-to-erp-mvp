# DataBridge MVP: Design Specification & Scope

## ✅ In-Scope (Supported)
- **Max Rows:** Up to 1,000,000 rows (Optimized for 200k).
- **Execution Mode:** Offline-first, single-machine.
- **Uniqueness Scope:** Within a single execution run only.
- **Memory Model:** In-memory buffer for performance.
- **Data Formats:** Excel (.xlsx), CSV.

## ❌ Out-of-Scope (Not Supported)
- **Incremental Load:** No tracking of previously processed files.
- **Cross-File Uniqueness:** Cannot check duplicates against older runs.
- **Streaming:** Not designed for real-time API streams.
- **Database:** No persistence layer (No SQL/NoSQL).

## Error Handling Semantics
- Validation is row-isolated.
- A single row failure does NOT stop the pipeline.
- Fatal errors are limited to configuration or IO failures only.
- Given the same input and config, output is deterministic.
