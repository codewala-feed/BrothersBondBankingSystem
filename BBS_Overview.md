# BROTHERS BOND BANKING SYSTEM (BBS)
---

## 1. System Overview

The **BrothersBond Banking System (BBS)** pilot represents a simplified enterprise banking ETL framework designed to simulate ingestion of product data from a legacy mainframe environment, process it through structured transformation layers, and produce integrated datasets for downstream consumption.

The architecture follows a **layered data pipeline model**:

Landing Zone → RAW → CLN → INT

Each layer has clearly defined responsibilities and uses a consistent audit and logging framework to maintain traceability and operational monitoring.

The framework is designed to support **multiple banking products**. The pilot implementation focuses on the **Credit Card (CC)** product but the design supports expansion to additional banking products.

---

## 2. Databases

Two databases are used to separate operational correction workflows from the analytical data pipeline.

### SQL Database

Name: `bbs_rtl_sql`

Purpose:

• Stores operational tables where Line-of-Business (LOB) teams can manually insert or modify records.
• Represents correction entries that later flow into the Hive pipeline.

Example table:

`bbs_cc_raw`

LOB users insert corrected records directly into this table.

---

### Hive Database

Name: `bbs_rtl_hive`

Purpose:

• Stores all data pipeline tables across RAW, CLN, and INT layers.
• Stores exception tables and operational logging tables.

---

## 3. Product Standardization

### Product Mnemonic

Each banking product has a two-character mnemonic used in table and script naming.

Credit Card mnemonic: `CC`

This mnemonic appears in table names, scripts, and product codes.

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

CC0USA – Standard Credit Card product for the USA branch
CC1USA – Premium Credit Card for the USA branch
CC2USA – Enterprise Credit Card for the USA branch
CC0IND – Standard Credit Card for the India branch
CC0ALL – Credit Card product applicable across all branches

Segment meanings:

PP identifies the product family.
V differentiates product variants such as standard, premium, or corporate.
CCC identifies the geographic applicability of the product.

This structure enables **regional product expansion and product tiering** without requiring schema changes.

---

## 4. Table Naming Standards

All pipeline tables follow the naming pattern:

bbs_<productMnemonic>_<layer>

For the Credit Card product, the tables are:

bbs_cc_raw_stg – staging table for raw ingestion
bbs_cc_raw – raw persistent table
bbs_cc_cln_stg – staging table for cleansing operations
bbs_cc_cln – cleansed data layer
bbs_cc_int – integrated product dataset
bbs_cc_excp_int – exception records rejected during validation

Supporting reference tables:

rej_reason_rules – validation rules by product code
bbs_product_map_int – product mapping reference

Operational monitoring table: `process_log`

---

## 5. Audit Column Standards

Every pipeline table from RAW through INT includes a standardized set of audit columns to maintain lineage and traceability.

batch_procs_date – batch execution date; used as a partition column
process_nm – name of the script performing the operation
created_by – indicates origin of the record (BATCH-RUN or LOB-MANUAL)
created_tm – timestamp when the record was created in the pipeline
change_ind – record lifecycle indicator

These audit attributes allow tracking of both automated ingestion and manual corrections across the system.

---

## 6. Change Indicator Model

The pipeline uses an **append-only historical data model**.

Records are never overwritten or deleted. Any update or deletion generates a new row describing the latest state.

change_ind values:

O – Original record
U – Updated record
D – Deleted record

Example record lifecycle:

AccountID | CreditLimit | change_ind
1001 | 50000 | O
1001 | 65000 | U
1001 | NULL | D

Deletion records remain in the same table and propagate through downstream layers, preserving historical lineage.

---

## 7. Mainframe Data Simulation

Because a real mainframe system is not available in the pilot environment, the pipeline simulates file ingestion through a shell script.

Script name: `mainframepull.sh`

Purpose:

Copy files from a mock source directory representing the mainframe environment into the landing zone.

Processing flow:

mock_mainframe → landing_zone

The landing zone serves as the entry point for batch ingestion.

---

## 8. Manual Data Corrections and Reprocessing

The framework supports operational corrections performed by Line-of-Business users.

LOB users insert corrected records directly into the SQL database table corresponding to the RAW structure of the product.

Example SQL table: `bbs_cc_raw (in bbs_rtl_sql)`

Manual inserts represent corrected versions of records that must be reprocessed.

To incorporate these corrections into the Hive pipeline, a corresponding Hive table is maintained.

Example Hive table: `bbs_cc_raw_reprocs`

A Sqoop job imports records from the SQL RAW table into this Hive reprocs table. During RAW pipeline execution, the reprocs records are incorporated into the ingestion stream so they proceed through the same transformation and validation logic as original data.

Records inserted through SQL corrections contain:

created_by = LOB-MANUAL
change_ind = U or D depending on the correction type.

---

## 9. Configuration-Driven Batch Processing

Batch execution is controlled through a configuration file.

Configuration file: `bbs_sales_dates.config`

This file contains the batch date that the pipeline should process.

Example content:

batch_procs_date=2026-03-15

Each pipeline script reads this configuration file and retrieves the batch_procs_date value. This ensures that every stage of the pipeline executes within the same batch context.

Using configuration-driven batch control allows rerunning historical batches without code modifications.

---

## 10. Script Naming Standards

All PySpark scripts follow **PascalCase naming**.

Structure: `<ProjectMnemonic><ProductMnemonic>_<Layer>.py`

Project mnemonic: `Bbs`

Product mnemonic: `Cc`

Examples:

BbsCc_Raw.py
BbsCc_Cln.py
BbsCc_Int.py

Shared logging module: `BbsProcessLog.py`

This naming convention ensures scripts are clearly identifiable and grouped by product and layer.

---

## 11. Process Logging Framework

The system includes a centralized logging mechanism to track execution statistics for **each pipeline script execution**.

Logging occurs **once per script**, not per table operation.

Table name: `process_log`

Columns:

process_nm – name of the script executed<br>
process_start_tm – start timestamp of the script<br>
process_end_tm – completion timestamp<br>
src_cnt – number of source records processed by the script<br>
rej_cnt – rejected records generated by the script<br>
trg_cnt – number of records successfully written by the script<br>
batch_procs_date – batch execution date<br>
created_tm – timestamp when the log record was inserted<br>

If rejection logic is not applicable for a script, rej_cnt is recorded as zero.

Example entries:

process_nm | src_cnt | rej_cnt | trg_cnt
BbsCc_Raw.py | 10000 | 0 | 10000
BbsCc_Cln.py | 10000 | 320 | 9680
BbsCc_Int.py | 9680 | 0 | 9680

---

## 12. Reusable Process Logging Script

A reusable Python module is implemented to simplify logging.

Script name: `BbsProcessLog.py`

Purpose:

Insert records into the process_log table.

All pipeline scripts import this module and call its logging function once their processing is completed.

Scripts using the module include:

BbsCc_Raw.py
BbsCc_Cln.py
BbsCc_Int.py

This approach ensures consistent logging across the pipeline.

---

## 13. Product-Specific Rejection Rules

Validation rules are maintained in a metadata table: `rej_reason_rules`

These rules are **defined per product_code**, allowing different products to maintain different validation criteria.

Since the rejection rules are product-specific, the validation process requires identification of the product associated with each record. This is achieved by joining the incoming data with the product mapping table.

Reference table: `bbs_product_map_int`

Processing logic:

Records are first associated with their product_code using the product mapping reference. The validation engine then joins the dataset with rej_reason_rules based on product_code to determine the applicable rejection rules.

Records failing validation are written to the exception table:

bbs_cc_excp_int

Valid records proceed to the CLN layer.

This design enables **product-specific validation logic without modifying the processing code**.

---

## 14. Pipeline Layer Responsibilities

### RAW Layer

Script: `BbsCc_Raw.py`

The RAW pipeline reads batch files from the landing zone, performs minimal structural parsing, and loads them into the RAW staging table before inserting them into the persistent RAW table. During this stage, records imported from the SQL correction table through the Hive reprocs table are incorporated into the ingestion stream. All records are stored with the appropriate audit metadata and batch processing context.

---

### CLN Layer

Script: `BbsCc_Cln.py`

The CLN pipeline reads records from the RAW table and performs cleansing operations such as data standardization, formatting adjustments, and validation checks. Product mapping is applied to identify the correct product_code, after which the dataset is evaluated against validation rules defined in the rej_reason_rules table. Records failing validation are written to the exception table bbs_cc_excp_int, while valid records are stored in the cleansed table bbs_cc_cln.

---

### INT Layer

Script: `BbsCc_Int.py`

The INT pipeline reads cleansed data from bbs_cc_cln and applies final business integration logic, including product mapping using the reference table bbs_product_map_int. The resulting curated dataset is written to the integrated table bbs_cc_int, which represents the final processed dataset ready for downstream analytics and reporting.

---

## 15. End-to-End Processing Flow

The overall system workflow begins with simulated mainframe files being transferred into the landing zone through the mainframepull.sh script. The RAW pipeline script ingests these files and combines them with any manual correction records imported from the SQL database. The CLN pipeline then performs cleansing and validation using product-specific rejection rules. Invalid records are redirected to the exception table while valid records are written to the cleansed data layer. The INT pipeline enriches the cleansed dataset using product mapping logic and writes the final integrated data into the INT table. Each pipeline script records execution metrics in the process_log table using the shared BbsProcessLog module. The batch processing date used throughout the pipeline is obtained from the configuration file bbs_sales_dates.config, ensuring consistent batch execution across all scripts.

---

## 16. Architectural Principles Implemented

The BBS pilot framework demonstrates several enterprise data engineering practices:

Layered ETL architecture separating ingestion, cleansing, and integration stages.
Append-only data design preserving full historical lineage.
Configuration-driven batch execution for flexible reruns.
Centralized process logging implemented at the script level.
Metadata-driven validation rules supporting product-specific data quality checks.
Integrated manual correction workflow using SQL reprocs tables.
Standardized naming conventions enabling scalable multi-product architecture.

---
