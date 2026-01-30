
---

# 📄 README.md

```md
# DataBridge

**Config-driven data validation and cleansing engine for structured business data.**

DataBridge is designed to validate, normalize, and audit large Excel or CSV datasets
_before_ they enter ERP, CRM, or financial systems.

---

## 🚀 What Problem Does DataBridge Solve?

Most enterprise data issues are detected **too late**:
- After import into ERP
- After reporting inconsistencies
- After financial or compliance damage

DataBridge acts as a **data quality gate**:
- Enforces schema and business rules
- Detects duplicates and inconsistencies
- Produces machine-readable error reports
- Works offline on a single machine

---

## ✅ What DataBridge Does

- Validates Excel (.xlsx) and CSV files
- Supports datasets up to **1,000,000 rows**
- Config-driven schema and rules (YAML)
- Composite and single-column uniqueness checks
- Conditional business rules
- Structured error output (JSON)
- Deterministic and reproducible execution

---

## ❌ What DataBridge Does NOT Do

- No incremental loads
- No cross-file uniqueness
- No database persistence
- No real-time or streaming processing
- No API or web interface (by design)

---

## 🧠 Execution Model

- Offline-first
- Single-machine
- Chunk-based processing for memory safety
- Row-isolated validation (no cascading failures)

---

## 📂 Project Structure

