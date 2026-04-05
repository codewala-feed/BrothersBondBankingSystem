---

# BROTHERS BOND BANKING SYSTEM (BBS)

---

## 1. System Overview

The **BrothersBond Banking System (BBS)** pilot version represents a simplified enterprise banking ETL framework designed to simulate ingestion of product data from a legacy mainframe environment, process it through structured transformation layers, and produce integrated datasets for downstream consumption.

The architecture follows a **layered and extended data pipeline model**:

Landing Zone → RAW → CLN → INT → WRK → SQL

Each layer has clearly defined responsibilities and uses a consistent audit and logging framework to maintain traceability and operational monitoring.

The framework is designed to support **multiple banking products**. The pilot implementation focuses on the **Credit Card (CC)** product but the design supports expansion to additional banking products.

---

## 2. Databases

Two databases are used to separate operational correction workflows from the analytical data pipeline.

### SQL Database

Name: `bbs_rtl_sql`

Purpose:

• Stores operational tables where Line-of-Business (LOB) teams can manually insert or modify records
• Stores final integrated and exception datasets for downstream consumption

Example tables:

`bbs_cc_raw`
`bbs_cc_int`
`bbs_cc_excp_int`

---

### Hive Database

Name: `bbs_rtl_hive`

Purpose:

• Stores all data pipeline tables across RAW, CLN, INT, and WRK layers
• Stores exception tables and operational logging tables

---

## 3. Product Standardization

### Product Mnemonic

Each banking product has a two-character mnemonic used in table and script naming.

Credit Card mnemonic: `CC`

---

### Product Code Structure

The system uses a standardized **6-character product code**.

Structure:

PPVCCC

Where:

PP – Product mnemonic (2 characters)
V – Product variant digit (1 character)
CCC – Country or branch identifier (3 characters)

Examples:

CC0USA
CC1USA
CC0IND
CC0ALL

---

## 4. Table Naming Standards

Pattern:

bbs_<productMnemonic>_<layer>

Examples:

bbs_cc_raw_stg
bbs_cc_raw
bbs_cc_cln_stg
bbs_cc_cln
bbs_cc_int
bbs_cc_excp_int

WRK tables:

bbs_cc_wrk_int
bbs_cc_excp_wrk_int

Reference tables:

rej_reason_rules
bbs_product_map_int

Operational table:

process_log

---

## 5. Audit Column Standards

batch_procs_date
process_nm
created_by
created_tm
change_ind

---

## 6. Change Indicator Model

Append-only model.

O – Original
U – Updated
D – Deleted

---

## 7. Mainframe Data Simulation

Script: `mainframepull.sh`

Copies files:

mock_mainframe → landing_zone

---

## 8. Manual Data Corrections and Reprocessing

LOB inserts data into:

`bbs_cc_raw` (SQL)

Imported into Hive using:

Script: `BbsOriginalRecCpySql_Import.py`

Flow:

SQL → Hive
bbs_cc_raw → bbs_cc_raw_reprocs

These records are merged into RAW processing.

---

## 9. Configuration-Driven Batch Processing

File: `bbs_sales_dates.config`

Used by all scripts to fetch:

batch_procs_date

---

## 10. Script Naming Standards

PascalCase:

BbsCc_Raw.py
BbsCc_Cln.py
BbsCc_Int.py

Generic scripts:

BbsOriginalRecCpySql_Import.py
BbsHive_ingestDB.py
BbsAdjustedRecCpySql_Export.py
BbsProcessLog.py

---

## 11. Process Logging Framework

Table: `process_log`

Columns:

process_nm
process_start_tm
process_end_tm
src_cnt
rej_cnt
trg_cnt
batch_procs_date
created_tm

Logging applies to:

BbsCc_Raw.py
BbsCc_Cln.py
BbsCc_Int.py
BbsOriginalRecCpySql_Import.py
BbsHive_ingestDB.py
BbsAdjustedRecCpySql_Export.py

(Shell scripts are not logged)

---

## 12. Reusable Process Logging Script

Script: `BbsProcessLog.py`

Used by all Python scripts to insert into process_log.

---

## 13. Product Mapping and Validation Framework

### Product Mapping Table

`bbs_product_map_int`

Used in CLN layer to derive:

product_code
sales_event_type
sales_level
product_desc

---

### Rejection Rules Table

`rej_reason_rules`

Columns:

product_code
rej_cd
rej_attr
rej_description
rej_technical_rule
rule_sequence
rule_layer
is_active
start_dt
end_dt
created_by
created_tm

---

### Validation Behavior

Rules applied dynamically per product_code.

Rejected records → bbs_cc_excp_int
Valid records → bbs_cc_cln

One row per rejection rule (Option A)

---

## 14. Pipeline Layer Responsibilities

### RAW Layer

Script: `BbsCc_Raw.py`

• Reads landing files
• Merges reprocs data
• Generates sales_unique_id
• Adds audit columns
• Loads into RAW

---

### CLN Layer

Script: `BbsCc_Cln.py`

• Reads RAW data
• Applies product mapping (join with bbs_product_map_int)
• Derives:

sales_event_type
sales_level
product_desc

• Applies rejection rules
• Writes:

Valid → bbs_cc_cln
Rejected → bbs_cc_excp_int

---

### INT Layer

Script: `BbsCc_Int.py`

• Reads CLN data
• Performs datatype standardization
• Updates audit columns
• Inserts into bbs_cc_int

No joins
No filtering
No product mapping

INT is append-only and represents the final curated dataset.

---

### WRK Layer

Script: `BbsHive_ingestDB.py`

Loads:

bbs_cc_int → bbs_cc_wrk_int
bbs_cc_excp_int → bbs_cc_excp_wrk_int

Purpose:

• Decouple export
• Enable retry
• Avoid partial loads

---

### SQL Export Layer

Script: `BbsAdjustedRecCpySql_Export.py`

Exports:

bbs_cc_wrk_int → bbs_cc_int (SQL)
bbs_cc_excp_wrk_int → bbs_cc_excp_int (SQL)

---

## 15. End-to-End Processing Flow

Mainframe → Landing
Landing → RAW
SQL → Hive (Reprocs via Sqoop Import)
RAW → CLN
CLN → INT
INT → WRK
WRK → SQL

Rejected records:

CLN → EXCP → WRK → SQL

---

## 16. Downstream Consumption

All downstream systems consume data from SQL tables:

bbs_cc_int
bbs_cc_excp_int

---

## 17. Architectural Principles Implemented

Layered ETL architecture
Append-only data model
Configuration-driven processing
Centralized logging
Metadata-driven validation
SQL-based reprocessing workflow
Decoupled export via WRK layer
Reusable generic scripts
Multi-product scalable design

---
