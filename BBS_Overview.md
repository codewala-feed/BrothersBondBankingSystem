# Brothers Bond Banking System (BBS)
---

## 1. System Overview

The **BrothersBond Banking System (BBS)** represents a simplified enterprise banking ETL framework designed to simulate ingestion of product sales data from a legacy mainframe environment, process it through structured transformation layers, and produce integrated datasets for downstream consumption.

The architecture follows a **layered data pipeline model**:

Landing Zone → RAW → CLN → INT

Each layer has clearly defined responsibilities and uses a consistent audit and logging framework to maintain traceability and operational monitoring.

The system is designed to support **multiple banking products** in the future. The pilot implementation focuses on **Credit Card (CC)** as the first product.

---

## 2. Databases

Two databases are used to separate operational correction workflows from the analytical data pipeline.

**SQL Database**

Name: `bbs_rtl_sql`

Purpose:

* Stores source tables where Line-of-Business (LOB) teams can manually insert or modify records.
* Represents operational correction entries that later flow into the data pipeline.

Example table:

`bbs_cc_raw`

LOB updates or inserts records directly into this table.

---

**Hive Database**

Name: `bbs_rtl_hive`

Purpose:

* Stores all pipeline tables across RAW, CLN, and INT layers.
* Stores exception tables and operational logging tables.

---

## 3. Product Standardization

### Product Mnemonic

Each banking product has a two-character mnemonic used in table and script naming.

Credit Card mnemonic:

CC

This mnemonic appears in table names, scripts, and product codes.

---

### Product Code Structure

The system uses a standardized **6-character product code** format.

Structure:

PPVCCC

Where:

PP – Product mnemonic (2 characters)
V – Product variant digit (1 character)
CCC – Country or branch identifier (3 characters)

Example product codes:

CC0USA – Base Credit Card product for the USA branch
CC1USA – Premium Credit Card for the USA branch
CC0IND – Standard Credit Card for the India branch
CC0ALL – Credit Card product applicable across all branches

Segment meanings:

PP identifies the product family.
V allows differentiation of product variants such as standard, premium, or corporate.
CCC identifies the geographic applicability of the product.

This structure supports **regional product expansion and product tiering** without changing table schemas.

---

## 4. Table Naming Standards

All pipeline tables follow a standardized naming convention:

bbs_<productMnemonic>_<layer>

For the Credit Card product, the tables are as follows:

bbs_cc_raw_stg – staging table for raw ingestion
bbs_cc_raw – raw persistent table
bbs_cc_cln_stg – staging table for cleansed processing
bbs_cc_cln – cleansed data layer
bbs_cc_int – integrated product dataset
bbs_cc_excp_int – exception records rejected during cleansing

Supporting reference tables:

rej_reason_rules – validation rule definitions
bbs_product_map_int – product mapping rules

Operational monitoring table:

process_log

---

## 5. Audit Column Standards

Every pipeline table from RAW through INT includes a standardized set of audit columns to maintain lineage and traceability.

batch_procs_date – batch execution date; used as a partition column
process_nm – name of the script performing the operation
created_by – indicates origin of the record (BATCH-RUN or LOB-MANUAL)
created_tm – timestamp when the record was created in the pipeline
change_ind – record lifecycle indicator

These columns ensure consistent tracking of both automated and manual record modifications across the system.

---

## 6. Change Indicator Model

The pipeline follows an **append-only data model**.

Records are never overwritten or physically deleted. Any update or deletion generates a new row representing the latest state.

change_ind values:

O – Original record
U – Updated record
D – Deleted record

Example record lifecycle:

AccountID | CreditLimit | change_ind
1001 | 50000 | O
1001 | 65000 | U
1001 | NULL | D

Deletion records remain within the same table and are propagated through downstream layers.

This design preserves complete historical traceability.

---

## 7. Mainframe Simulation

Because a real mainframe system is not available in the pilot environment, the pipeline simulates file ingestion through a shell script.

Script name:

mainframepull.sh

Purpose:

Copy files from a mock source directory representing the mainframe environment into the system’s landing zone.

Flow:

mock_mainframe → landing_zone

The landing zone acts as the entry point for batch ingestion.

---

## 8. Manual Data Corrections and Reprocessing

The system supports operational corrections performed by Line-of-Business users.

LOB users insert corrected records directly into the SQL database table corresponding to the product’s RAW structure.

Example SQL table:

bbs_cc_raw (in bbs_rtl_sql)

These manual updates represent corrected versions of records that must be reprocessed.

To feed these corrections into the Hive pipeline, a corresponding reprocessing table exists in Hive.

Example Hive table:

bbs_cc_raw_reprocs

A Sqoop import job transfers records from the SQL table into this Hive reprocs table. During the RAW pipeline execution, the records from the reprocs table are incorporated into the ingestion stream so they flow through the same validation and transformation steps as original records.

Records inserted through SQL corrections will have:

created_by = LOB-MANUAL

and change_ind values of U or D depending on the correction type.

---

## 9. Configuration-Driven Batch Processing

The system uses a configuration file to control the batch processing date.

Configuration file name:

bbs_sales_dates.config

This file stores the batch processing date that scripts should use during execution.

Example configuration content:

batch_procs_date=2026-03-15

Each pipeline script reads this configuration file and retrieves the batch_procs_date value. This ensures that all stages of the pipeline operate using the same batch context.

Using a configuration file enables rerunning pipelines for specific historical batch dates without modifying code.

---

## 10. Script Naming Standards

All PySpark scripts follow a **PascalCase naming convention**.

Structure:

<ProjectMnemonic><ProductMnemonic>_<Layer>.py

Project mnemonic:

Bbs

Product mnemonic:

Cc

Examples:

BbsCc_Raw.py
BbsCc_Cln.py
BbsCc_Int.py

Shared logging module:

BbsProcessLog.py

This standard ensures scripts are easily identifiable and grouped by product.

---

## 11. Process Logging Framework

The pipeline includes a centralized logging mechanism to track execution statistics.

Table name:

process_log

Columns:

process_nm – name of the script executed
process_start_tm – start timestamp of execution
process_end_tm – completion timestamp
src_cnt – count of source records read
rej_cnt – count of rejected records
trg_cnt – count of records successfully written to target
batch_procs_date – batch execution date
created_tm – timestamp when log entry was inserted

If rejection logic is not applicable in a process, rej_cnt will be recorded as zero.

---

## 12. Reusable Process Logging Script

A reusable Python module is created to simplify logging across all pipeline scripts.

Script name:

BbsProcessLog.py

This module provides functionality to insert records into the process_log table.

All pipeline scripts import this module and call its logging function after completing their processing steps.

Example scripts that import it:

BbsCc_Raw.py
BbsCc_Cln.py
BbsCc_Int.py

This ensures consistent operational logging across the entire pipeline.

---

## 13. Pipeline Layer Responsibilities

**RAW Layer**

Script: BbsCc_Raw.py

The RAW pipeline reads batch files from the landing zone, performs minimal structural parsing, and loads them into the RAW tables. At this stage, the system also incorporates records from the Hive reprocs table that originated from manual corrections in the SQL database. The combined dataset is inserted into the persistent RAW table while maintaining audit metadata and batch processing context.

---

**CLN Layer**

Script: BbsCc_Cln.py

The CLN pipeline reads records from the RAW table and performs cleansing operations such as data standardization, formatting adjustments, and validation checks. Validation logic is driven by the rules stored in the rej_reason_rules table. Records failing validation are redirected to the exception table bbs_cc_excp_int, while valid records are inserted into the cleansed table bbs_cc_cln.

---

**INT Layer**

Script: BbsCc_Int.py

The INT pipeline reads cleansed data from bbs_cc_cln and applies product mapping logic using reference data stored in bbs_product_map_int. The result is written to the final integrated dataset table bbs_cc_int, which represents the curated version of the product data ready for downstream consumption.

---

## 14. End-to-End Processing Flow

The overall system workflow proceeds as follows:

A simulated mainframe environment produces data files that are transferred to the landing zone using the mainframepull.sh script. The RAW pipeline reads these files and ingests them into the raw data tables while also incorporating any manual correction records imported from the SQL reprocs table. The CLN pipeline then validates and cleanses the raw data according to metadata-driven validation rules. Invalid records are captured in the exception table while valid records move to the cleansed layer. Finally, the INT pipeline enriches the cleansed data with product mappings and writes the results to the integrated data layer. Throughout the process, each script records execution metrics in the process_log table using the shared BbsProcessLog logging module. The batch processing date used across all scripts is obtained from the configuration file bbs_sales_dates.config, ensuring consistent batch execution control.

---

## 15. Architectural Principles Implemented

The BBS pilot framework incorporates several enterprise data engineering principles:

Layered ETL architecture separating ingestion, cleansing, and integration stages.
Append-only historical data design ensuring complete record lineage.
Configuration-driven batch processing enabling controlled reruns.
Centralized operational logging through a reusable logging module.
Metadata-driven validation rules allowing rule changes without modifying code.
Manual correction workflow integrated into the pipeline through reprocs tables.
Scalable naming conventions supporting multiple products in the future.

---
