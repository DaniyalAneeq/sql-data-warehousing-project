# Naming Conventions

This document defines the naming conventions used for schemas, tables, views, columns, and other database objects in the Data Warehouse. Consistent naming improves readability, maintainability, and collaboration across the data platform.

---

## Table of Contents

1. [General Principles](#general-principles)
2. [Table Naming Conventions](#table-naming-conventions)
   - [Bronze Layer](#bronze-layer)
   - [Silver Layer](#silver-layer)
   - [Gold Layer](#gold-layer)
3. [Column Naming Conventions](#column-naming-conventions)
   - [Surrogate Keys](#surrogate-keys)
   - [Technical Columns](#technical-columns)
4. [Stored Procedure Naming](#stored-procedure-naming)

---

# General Principles

- **Naming Style**: Use `snake_case` with lowercase letters and underscores (`_`) to separate words.
- **Language**: Use English for all database objects.
- **Clarity**: Use clear and descriptive names that reflect the business meaning.
- **Avoid Reserved Words**: Do not use SQL reserved keywords as object names.
- **Consistency**: Follow the same naming patterns across all layers of the data warehouse.

---

# Table Naming Conventions

## Bronze Layer

The **Bronze layer** stores raw data ingested from source systems with minimal or no transformation.

### Pattern
<sourcesystem>_<entity>

### Rules
- Table names must begin with the **source system name**.
- Table names should match the **original source table name** whenever possible.
- Avoid renaming fields unless necessary.

### Examples

| Table Name | Description |
|------------|-------------|
| `crm_customer_info` | Customer information from CRM |
| `erp_orders` | Order data from ERP system |

---

## Silver Layer

The **Silver layer** contains cleaned, standardized, and transformed data from the Bronze layer.

### Pattern
<sourcesystem>_<entity>


### Rules
- Maintain the **source system prefix** for traceability.
- Apply **data cleansing, normalization, and transformations**.
- Table names should remain consistent with the source structure when possible.

### Examples

| Table Name | Description |
|------------|-------------|
| `crm_customer_info` | Cleaned CRM customer data |
| `erp_orders` | Standardized ERP order data |

---

## Gold Layer

The **Gold layer** represents business-ready data models optimized for analytics and reporting.

### Pattern
<category>_<entity>


### Components

- **`category`** → Indicates the table type (dimension, fact, report).
- **`entity`** → Describes the business entity.

### Examples

| Table Name | Description |
|------------|-------------|
| `dim_customers` | Customer dimension |
| `dim_products` | Product dimension |
| `fact_sales` | Sales transaction fact table |
| `report_sales_monthly` | Monthly sales reporting table |

---

## Category Prefix Glossary

| Prefix | Meaning | Example |
|------|------|------|
| `dim_` | Dimension table | `dim_customer`, `dim_product` |
| `fact_` | Fact table | `fact_sales` |
| `report_` | Reporting table | `report_sales_monthly` |

---

# Column Naming Conventions

## Surrogate Keys

All dimension tables must use **surrogate keys** as primary keys.

### Pattern
<entity>_key


### Rules
- Surrogate keys must end with `_key`.
- They should uniquely identify each record in a dimension table.

### Example

| Column Name | Description |
|-------------|-------------|
| `customer_key` | Surrogate key in `dim_customers` |
| `product_key` | Surrogate key in `dim_products` |

---

## Technical Columns

Technical or metadata columns store information related to data warehouse processing.

### Pattern
dwh_<column_name>


### Rules
- All system-generated columns must start with the prefix `dwh_`.
- These columns track metadata such as load dates, update timestamps, or pipeline information.

### Examples

| Column Name | Description |
|-------------|-------------|
| `dwh_load_date` | Date when the record was loaded |
| `dwh_update_date` | Last update timestamp |
| `dwh_source_system` | Source system identifier |

---

# Stored Procedure Naming

Stored procedures used for loading or transforming data must follow a consistent naming pattern.

### Pattern
load_<layer>


### Rules
- Stored procedures should clearly indicate the **layer they load**.
- The name should reflect the **purpose of the pipeline step**.

### Examples

| Procedure Name | Description |
|---------------|-------------|
| `load_bronze` | Loads raw data into the Bronze layer |
| `load_silver` | Transforms and loads data into the Silver layer |
| `load_gold` | Loads analytical models into the Gold layer |

---
