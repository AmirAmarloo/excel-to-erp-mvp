# DataBridge Error Model

This document defines the structured error model used by DataBridge for
machine-readable validation, auditing, and ERP/CRM integration.

---

## 🎯 Design Goals

- Deterministic and reproducible error reporting
- Row-isolated validation (one row failure never blocks others)
- Machine-readable structure (JSON-first)
- Human-readable messages for analysts
- ERP / ETL friendly severity levels

---

## 🧱 Error Object Structure

Each validation error is represented as a structured object:

```json
{
  "Row_Number": 447,
  "Column_Name": "invoice_number",
  "Invalid_Value": "INV-91",
  "Error_Type": "COMPOSITE_DUPLICATE",
  "Severity": 1,
  "Error_Message": "Duplicate of row 93"
}
