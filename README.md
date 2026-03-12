# 🏛️ SQL Data Warehousing Project
[![SQL Server](https://img.shields.io/badge/SQL%20Server-T--SQL-CC2927?logo=microsoftsqlserver&logoColor=white)](https://www.microsoft.com/en-us/sql-server)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%20→%20Silver%20→%20Gold)-FFD700)](https://docs.databricks.com/en/lakehouse/medallion.html)
[![Schema](https://img.shields.io/badge/Data%20Model-Star%20Schema-4CAF50)](https://en.wikipedia.org/wiki/Star_schema)
[![License](https://img.shields.io/badge/License-MIT-blue)](LICENSE)
An end-to-end **SQL Data Warehouse** built on Microsoft SQL Server, implementing the **Medallion Architecture** (Bronze → Silver → Gold) with full ETL pipelines, data quality checks, and a star schema presentation layer ready for BI consumption.
---
## 📑 Table of Contents
- [Overview](#-overview)
- [Data Architecture](#-data-architecture)
- [Project Structure](#-project-structure)
- [Source Data](#-source-data)
- [Data Flow & ETL Pipeline](#-data-flow--etl-pipeline)
  - [Bronze Layer — Raw Ingestion](#bronze-layer--raw-ingestion)
  - [Silver Layer — Cleanse & Transform](#silver-layer--cleanse--transform)
  - [Gold Layer — Star Schema / Presentation](#gold-layer--star-schema--presentation)
- [Data Quality Checks](#-data-quality-checks)
- [Star Schema Model](#-star-schema-model)
- [Naming Conventions](#-naming-conventions)
- [Project Requirements & Prerequisites](#-project-requirements--prerequisites)
- [Getting Started](#-getting-started)
- [Repository Structure](#-repository-structure)
- [Key Technical Decisions](#-key-technical-decisions)
- [Future Improvements](#-future-improvements)
---
## 🔭 Overview
This project demonstrates how to design and build a **production-style SQL Data Warehouse** from scratch using only Microsoft SQL Server (T-SQL). It ingests raw CSV data from two simulated enterprise source systems — a **CRM** and an **ERP** — applies multi-stage transformations, enforces data quality rules, and produces a clean, analytics-ready **star schema** that can be plugged directly into reporting tools like Power BI or Tableau.
**What this project showcases:**
- End-to-end ETL pipeline design using pure T-SQL stored procedures
- Medallion (three-tier) architecture: Bronze → Silver → Gold
- Data quality assessment and cleansing patterns (deduplication, null handling, type coercion, value standardization)
- Star schema data modeling with surrogate keys (dimension + fact tables)
- Multi-source data integration (CRM + ERP merged into a unified customer and product dimension)
- Consistent naming conventions and self-documenting SQL code
---
## 🏗️ Data Architecture
This project follows the **Medallion Architecture**, a layered data design pattern where each layer serves a specific purpose and increases data quality progressively.
```
┌──────────────────────────────────────────────────────────────────────────┐
│                         SOURCE SYSTEMS                                   │
│                                                                          │
│   ┌─────────────────────┐        ┌─────────────────────┐                │
│   │        CRM          │        │        ERP          │                │
│   │  ─ cust_info.csv    │        │  ─ CUST_AZ12.csv    │                │
│   │  ─ prd_info.csv     │        │  ─ LOC_A101.csv     │                │
│   │  ─ sales_details.csv│        │  ─ PX_CAT_G1V2.csv  │                │
│   └──────────┬──────────┘        └──────────┬──────────┘                │
└──────────────┼───────────────────────────────┼──────────────────────────┘
               │         BULK INSERT           │
               ▼                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    🥉 BRONZE LAYER  (schema: bronze)                     │
│                     Raw / Landing Zone — No transformation               │
│                                                                          │
│   bronze.crm_cust_info       bronze.erp_cust_az12                       │
│   bronze.crm_prd_info        bronze.erp_loc_a101                        │
│   bronze.crm_sales_details   bronze.erp_px_cat_g1v2                     │
│                                                                          │
│   Loaded via: EXEC bronze.load_bronze                                    │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │  TRUNCATE + INSERT (full refresh)
                                │  Cleanse / Standardize / Deduplicate
                                ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    🥈 SILVER LAYER  (schema: silver)                     │
│                    Cleaned, Standardized, Audit-stamped                  │
│                                                                          │
│   silver.crm_cust_info       silver.erp_cust_az12                       │
│   silver.crm_prd_info        silver.erp_loc_a101                        │
│   silver.crm_sales_details   silver.erp_px_cat_g1v2                     │
│                                                                          │
│   + dwh_create_date metadata column on all tables                       │
│   Loaded via: EXEC silver.load_silver                                    │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │  Business logic / surrogate keys / JOINs
                                │  Multi-source merge (CRM + ERP)
                                ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    🥇 GOLD LAYER  (schema: gold)                         │
│                    Star Schema — Business-Ready Views                    │
│                                                                          │
│   ┌────────────────────┐   ┌────────────────────┐                       │
│   │  gold.dim_customers│   │  gold.dim_products │                       │
│   │  (customer_key PK) │   │  (product_key PK)  │                       │
│   └────────┬───────────┘   └─────────┬──────────┘                       │
│            │                         │                                   │
│            └──────────┬──────────────┘                                   │
│                       ▼                                                  │
│              ┌────────────────────┐                                      │
│              │  gold.fact_sales   │                                      │
│              │  (customer_key FK) │                                      │
│              │  (product_key  FK) │                                      │
│              └────────────────────┘                                      │
│                                                                          │
│   Implemented as SQL VIEWS (no physical storage)                         │
└──────────────────────────────────────────────────────────────────────────┘
```
---
## 📁 Project Structure
```
sql-data-warehousing-project/
│
├── source/                          # Raw CSV source data files
│   ├── source_crm/
│   │   ├── cust_info.csv            # CRM customer master
│   │   ├── prd_info.csv             # CRM product catalog
│   │   └── sales_details.csv        # CRM sales transactions
│   └── source_erp/
│       ├── CUST_AZ12.csv            # ERP customer supplement (birthdate, gender)
│       ├── LOC_A101.csv             # ERP location / country mapping
│       └── PX_CAT_G1V2.csv         # ERP product category hierarchy
│
├── scripts/
│   ├── database_init.sql            # Create DataWarehouse DB + 3 schemas
│   ├── bronze/
│   │   ├── ddl_bronze.sql           # CREATE TABLE statements for bronze layer
│   │   └── procedure_load.sql       # bronze.load_bronze — BULK INSERT from CSV
│   ├── silver/
│   │   ├── ddl_silver.sql           # CREATE TABLE statements for silver layer
│   │   └── procedure_load.sql       # silver.load_silver — cleanse + transform
│   └── gold/
│       └── ddl_gold.sql             # Gold layer VIEWS (dim_customers, dim_products, fact_sales)
│
├── quality-checks/                  # Standalone DQ diagnostic & transformation scripts
│   ├── crm_cust_info                # Duplicate/null checks, standardization preview
│   ├── crm_prd_info                 # Product key parsing, LEAD-based end-date derivation
│   ├── crm_sales_details            # Date validation, sales math repair
│   ├── erp_cust_az12                # NAS-prefix removal, future-date birthdate check
│   ├── erp_loc_a101                 # Dash removal, country code normalization
│   └── erp_px_cat_g1v2             # Category passthrough validation
│
├── convention.md                    # Full naming convention documentation
└── README.md                        # This file
```
---
## 📦 Source Data
The project ingests data from two simulated enterprise source systems via flat CSV files.
### CRM Source (`source/source_crm/`)
| File | Table Loaded | Key Columns | Description |
|---|---|---|---|
| `cust_info.csv` | `bronze.crm_cust_info` | `cst_id`, `cst_key`, `cst_firstname`, `cst_lastname`, `cst_material_status`, `cst_gndr`, `cst_create_date` | Customer master records with marital status and gender codes |
| `prd_info.csv` | `bronze.crm_prd_info` | `prd_id`, `prd_key`, `prd_nm`, `prd_cost`, `prd_line`, `prd_start_dt`, `prd_end_dt` | Product catalog with line codes (M/R/S/T) and validity dates |
| `sales_details.csv` | `bronze.crm_sales_details` | `sls_ord_num`, `sls_ord_key`, `sls_cust_id`, `sls_order_dt`, `sls_ship_dt`, `sls_due_dt`, `sls_sales`, `sls_quantity`, `sls_price` | Sales order transactions with dates stored as YYYYMMDD integers |
### ERP Source (`source/source_erp/`)
| File | Table Loaded | Key Columns | Description |
|---|---|---|---|
| `CUST_AZ12.csv` | `bronze.erp_cust_az12` | `cid`, `bdate`, `gen` | Supplemental customer data — birthdate and gender |
| `LOC_A101.csv` | `bronze.erp_loc_a101` | `cid`, `cntry` | Customer-to-country mapping |
| `PX_CAT_G1V2.csv` | `bronze.erp_px_cat_g1v2` | `id`, `cat`, `subcat`, `maintenance` | Product category and subcategory hierarchy |
---
## 🔄 Data Flow & ETL Pipeline
### Bronze Layer — Raw Ingestion
**Script:** `scripts/bronze/procedure_load.sql` → Procedure: `bronze.load_bronze`
The Bronze layer is the raw **landing zone**. Data is loaded as-is from CSV files using `BULK INSERT` with no transformations applied. The procedure:
- Truncates each table before reloading (full refresh pattern)
- Loads all 6 source tables sequentially (3 CRM + 3 ERP)
- Measures and prints per-table load duration and overall batch summary
- Wraps execution in a `TRY/CATCH` block with detailed error logging
```sql
EXEC bronze.load_bronze;
```
> ⚠️ **Note:** The `BULK INSERT` paths in the procedure are hardcoded to a local development machine. Update the file paths to match your environment before executing.
---
### Silver Layer — Cleanse & Transform
**Script:** `scripts/silver/procedure_load.sql` → Procedure: `silver.load_silver`
The Silver layer applies **data quality rules, standardization, and enrichment** to every source table. Key transformations per table:
**`silver.crm_cust_info`**
- `TRIM()` applied to `cst_firstname` and `cst_lastname` to remove whitespace
- Marital status normalized: `'S'` → `'Single'`, `'M'` → `'Married'`, else `'n/a'`
- Gender normalized: `'F'` → `'Female'`, `'M'` → `'Male'`, else `'n/a'`
- Deduplication via `ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)` — keeps the most recent record per customer
**`silver.crm_prd_info`**
- Category ID extracted from `prd_key` using `SUBSTRING` and `REPLACE` (first 5 chars, dashes → underscores)
- Product key cleaned (characters 7 onward)
- NULL cost replaced with `0` via `COALESCE`
- Product line decoded: `'M'` → `'Mountain'`, `'R'` → `'Road'`, `'S'` → `'Other Sales'`, `'T'` → `'Touring'`
- End date enriched using `LEAD(prd_start_dt) - 1` over product key windows (SCD-style validity)
**`silver.crm_sales_details`**
- Dates converted from YYYYMMDD integers to `DATE` type (invalid values → `NULL`)
- Sales amount repaired: enforces `sls_sales = sls_quantity × |sls_price|` when inconsistent
- Unit price derived from `sls_sales / sls_quantity` when price is zero, negative, or NULL
**`silver.erp_cust_az12`**
- Legacy `'NAS'` prefix stripped from `cid` using `SUBSTRING`
- Future birthdates set to `NULL`
- Gender standardized: `'F'`/`'FEMALE'` → `'Female'`, `'M'`/`'MALE'` → `'Male'`, else `'n/a'`
**`silver.erp_loc_a101`**
- Dashes removed from `cid` using `REPLACE`
- Country codes expanded: `'DE'` → `'Germany'`, `'US'`/`'USA'` → `'United States'`, blanks/NULLs → `'n/a'`
**`silver.erp_px_cat_g1v2`**
- Pass-through (clean at source — minimal transformation needed)
All Silver tables include a `dwh_create_date DATETIME2 DEFAULT GETDATE()` audit column populated automatically on insert.
```sql
EXEC silver.load_silver;
```
---
### Gold Layer — Star Schema / Presentation
**Script:** `scripts/gold/ddl_gold.sql`
The Gold layer implements a classic **star schema** using SQL `VIEW`s — no physical data is stored; results are always computed at query time. It integrates both source systems into unified, analytics-ready objects.
**`gold.dim_customers`** — Unified Customer Dimension
- Merges `silver.crm_cust_info` (primary) with `silver.erp_cust_az12` (birthdate) and `silver.erp_loc_a101` (country)
- Gender priority logic: CRM value preferred → ERP fallback → `'n/a'`
- `customer_key` surrogate key generated via `ROW_NUMBER() OVER (ORDER BY cst_key)`
**`gold.dim_products`** — Enriched Product Dimension
- Joins `silver.crm_prd_info` with `silver.erp_px_cat_g1v2` on the extracted `cat_id`
- Filters to **currently active products only** (`WHERE prd_end_dt IS NULL`)
- `product_key` surrogate key generated via `ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key)`
**`gold.fact_sales`** — Sales Transaction Fact Table
- References `gold.dim_products` and `gold.dim_customers` via surrogate keys (`product_key`, `customer_key`)
- Exposes order number, order/ship/due dates, sales amount, quantity, and unit price
---
## ✅ Data Quality Checks
The `quality-checks/` directory contains standalone diagnostic SQL scripts for every source table. Each script follows a consistent three-phase structure:
1. **Diagnostic Queries** — Detect problems in the bronze layer (should return 0 rows in a clean dataset)
   - Duplicate primary keys or NULL PKs
   - Leading/trailing whitespace in string columns
   - Invalid date integers (out of range, wrong length)
   - Logical date order violations (order date > ship date)
   - Sales math inconsistencies (`sales ≠ quantity × price`)
2. **Preview Queries** — Show what the transformed output would look like before committing
3. **Final INSERT** — Execute the full cleanse + load into the silver table
| Script | Key Checks Applied |
|---|---|
| `crm_cust_info` | Duplicate `cst_id`, whitespace in names, gender/marital standardization |
| `crm_prd_info` | Product key parsing, LEAD-based end date, NULL cost handling |
| `crm_sales_details` | Date integer validation (range 1900–2050), sales math repair, price derivation |
| `erp_cust_az12` | NAS prefix, future birthdates, gender normalization |
| `erp_loc_a101` | Dash removal from IDs, country code expansion |
| `erp_px_cat_g1v2` | Structural passthrough validation |
---
## ⭐ Star Schema Model
```
                    ┌───────────────────────┐
                    │   gold.dim_customers  │
                    │───────────────────────│
                    │ customer_key   (PK)   │
                    │ customer_id           │
                    │ customer_number       │
                    │ first_name            │
                    │ last_name             │
                    │ country               │
                    │ marital_status        │
                    │ gender                │
                    │ birthdate             │
                    │ create_date           │
                    └──────────┬────────────┘
                               │ FK: customer_key
                               │
         ┌─────────────────────▼──────────────────────┐
         │              gold.fact_sales               │
         │────────────────────────────────────────────│
         │ order_number                               │
         │ product_key    ──────────────────────────► FK
         │ customer_key   ◄ FK                        │
         │ order_date                                 │
         │ shipping_date                              │
         │ due_date                                   │
         │ sales_amount                               │
         │ quantity                                   │
         │ price                                      │
         └─────────────────────┬──────────────────────┘
                               │ FK: product_key
                               │
                    ┌──────────▼────────────┐
                    │   gold.dim_products   │
                    │───────────────────────│
                    │ product_key    (PK)   │
                    │ product_id            │
                    │ product_number        │
                    │ product_name          │
                    │ category_id           │
                    │ category              │
                    │ subcategory           │
                    │ maintenance           │
                    │ cost                  │
                    │ product_line          │
                    │ start_date            │
                    └───────────────────────┘
```
---
## 📐 Naming Conventions
All database objects follow a consistent `snake_case` naming standard documented in [`convention.md`](convention.md).
| Layer | Object Type | Pattern | Example |
|---|---|---|---|
| Bronze | Tables | `<source>_<entity>` | `crm_cust_info`, `erp_loc_a101` |
| Silver | Tables | `<source>_<entity>` | `silver.crm_cust_info` |
| Gold | Dimensions | `dim_<entity>` | `gold.dim_customers` |
| Gold | Facts | `fact_<entity>` | `gold.fact_sales` |
| Gold | Reports | `report_<entity>` | `report_sales_monthly` |
| All | Surrogate Keys | `<entity>_key` | `customer_key`, `product_key` |
| All | Audit Columns | `dwh_<name>` | `dwh_create_date` |
| All | Load Procedures | `load_<layer>` | `bronze.load_bronze` |
---
## 🛠️ Project Requirements & Prerequisites
### Software Requirements
| Requirement | Details |
|---|---|
| **Database Engine** | Microsoft SQL Server 2016 or later (T-SQL required) |
| **SQL Client** | SQL Server Management Studio (SSMS) 18+ or Azure Data Studio |
| **Permissions** | `BULK INSERT` permission, `CREATE DATABASE` rights, `db_owner` on `DataWarehouse` |
| **Storage** | Sufficient disk space for the DataWarehouse database and CSV source files |
### Skills / Knowledge Prerequisites
- Intermediate T-SQL (CTEs, window functions, stored procedures, `BULK INSERT`)
- Basic data warehousing concepts (star schema, dimension/fact tables, surrogate keys)
- Familiarity with ETL patterns (extract, transform, load)
### CSV File Requirements
- Files must be comma-delimited (`,`) with a header row
- Headers are skipped via `FIRSTROW = 2` in the `BULK INSERT` statement
- Files must be accessible from the SQL Server process (local paths or network UNC paths)
---
## 🚀 Getting Started
Follow these steps in order to set up the warehouse from scratch.
### Step 1 — Clone the Repository
```bash
git clone https://github.com/DaniyalAneeq/sql-data-warehousing-project.git
cd sql-data-warehousing-project
```
### Step 2 — Initialize the Database
Run the initialization script to create the `DataWarehouse` database and the `bronze`, `silver`, and `gold` schemas.
> ⚠️ **Warning:** This script drops and recreates the entire `DataWarehouse` database. All existing data will be lost. Ensure you have a backup before running.
```sql
-- Run in SSMS connected to master
-- File: scripts/database_init.sql
```
### Step 3 — Create Bronze Layer Tables
```sql
-- File: scripts/bronze/ddl_bronze.sql
```
### Step 4 — Update File Paths for BULK INSERT
Open `scripts/bronze/procedure_load.sql` and update the hardcoded file paths to point to where you have stored the CSV files from the `source/` directory on your local machine.
```sql
-- Example — update this path:
FROM 'C:\\your-path\\source\\source_crm\\cust_info.csv'
```
### Step 5 — Create and Execute the Bronze Load Procedure
```sql
-- File: scripts/bronze/procedure_load.sql
EXEC bronze.load_bronze;
```
### Step 6 — Create Silver Layer Tables
```sql
-- File: scripts/silver/ddl_silver.sql
```
### Step 7 — Create and Execute the Silver Load Procedure
```sql
-- File: scripts/silver/procedure_load.sql
EXEC silver.load_silver;
```
### Step 8 — Create the Gold Layer Views
```sql
-- File: scripts/gold/ddl_gold.sql
```
### Step 9 — Validate the Output
```sql
-- Verify Gold layer outputs
SELECT TOP 10 * FROM gold.dim_customers;
SELECT TOP 10 * FROM gold.dim_products;
SELECT TOP 10 * FROM gold.fact_sales;
```
---
## 🔑 Key Technical Decisions
**Full Refresh over Incremental Load** — Both Bronze and Silver use a truncate-and-reload pattern. This keeps the ETL logic simple and deterministic, suitable for the dataset volumes in this project. Incremental/merge patterns can be added for production scale.
**Views for the Gold Layer** — The Gold layer is implemented as `CREATE OR ALTER VIEW` rather than physical tables. This ensures the Gold layer always reflects the latest Silver data without needing a separate load step. For large production workloads, these can be materialized as tables.
**CRM as the Primary Source** — Where CRM and ERP both carry the same attribute (e.g., gender), CRM is treated as authoritative and ERP is used only as a fallback. This is enforced via `CASE WHEN crm_value != 'n/a' THEN crm_value ELSE erp_value END`.
**Surrogate Keys via ROW_NUMBER()** — Surrogate keys are generated at query time using `ROW_NUMBER()` in the Gold views. They are stable within a single query run but would need a persistent sequence or identity column if persistence across sessions is required.
**BULK INSERT for Bronze Loading** — Raw data ingestion uses `BULK INSERT` for maximum throughput. In a production environment this would typically be replaced with Azure Data Factory pipelines, SSIS packages, or PolyBase external tables.
---
## 🔮 Future Improvements
- **Incremental Loading** — Implement watermark-based or CDC-based incremental loads in Silver to handle growing data volumes without full table scans
- **Persistent Surrogate Keys** — Replace `ROW_NUMBER()` views with physical dimension tables using `IDENTITY` or `SEQUENCE` for stable surrogate keys across loads
- **SCD Type 2** — Add `valid_from` / `valid_to` date columns to dimension tables to track historical attribute changes
- **Logging Table** — Replace `PRINT`-based logging with writes to a persistent `audit.pipeline_log` table for operational monitoring
- **Transaction Control** — Wrap each layer's load in explicit `BEGIN TRANSACTION / COMMIT / ROLLBACK` for atomicity
- **Parameterized File Paths** — Accept CSV directory path as a parameter in `bronze.load_bronze` to eliminate hardcoded paths
- **Analytical Queries / Reports** — Add a `report_` layer with pre-aggregated views (e.g., monthly sales by region, product line revenue trends)
- **BI Integration** — Connect `gold.dim_*` and `gold.fact_*` views directly to Power BI for interactive dashboards
---
## 📄 License
This project is open source and available under the [MIT License](LICENSE).
---
## 🙋 Author
## Author
---
<table>
  <tr>
    <td align="center">
      <a href="https://github.com/DaniylAneeq">
        <img src="[https://avatars.githubusercontent.com/u/167881750?s=400&u=3524972ce077574cb53b4b365125a4bc23144785&v=4](https://avatars.githubusercontent.com/u/167881750?v=4)" width="100px;" alt="Danial Aneeq"/>
        <br />
        <sub><b>DANIAL ANEEQ AHMED</b></sub>
      </a>
    </td>
  </tr>
</table>
**AI & Data Engineer** | SQL Server • Data Warehousing • ETL Design
| | |
|---|---|
| 🐙 **GitHub** | [@danialaneeq](https://github.com/DaniylAneeq) |
| 💼 **LinkedIn** | [linkedin.com/in/danialaneeq](https://linkedin.com/in/danialaneeq) |
| 📧 **Email** | [daniyalaneeqahmed@gmail.com](mailto:daniyalaneeqahmed@gmail.com) |
> This project is my hands-on implementation of a full modern data warehouse using SQL Server — from raw ingestion to analytics-ready star schema.
---
> *Built with pure T-SQL on Microsoft SQL Server — no external dependencies required.*
