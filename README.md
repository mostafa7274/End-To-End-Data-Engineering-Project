# FMCG Data Engineering Pipeline (Lakehouse Architecture)

## Overview

This project is an end-to-end data engineering pipeline built on Databricks, simulating a real-world FMCG use case where a large retail company acquires a smaller one.

The goal is to consolidate data from both companies into a unified Lakehouse architecture, enabling consistent analytics and business insights.

---

## Architecture

The pipeline follows the **Medallion Architecture**:

* **Bronze Layer** → Raw ingestion from multiple sources
* **Silver Layer** → Data cleaning, validation, and transformation
* **Gold Layer** → Aggregated, business-ready datasets for analytics

---

## Tech Stack

* Python
* SQL
* Apache Spark
* Databricks
* Amazon S3
* Lakehouse Architecture
* BI Dashboard (for visualization)

---

## Pipeline Workflow

1. Ingest raw data from both companies into the Bronze layer
2. Clean and standardize schemas in the Silver layer
3. Resolve data inconsistencies (e.g., customer IDs, formats)
4. Build unified, analytics-ready tables in the Gold layer
5. Serve data to BI dashboards for reporting and insights

---

##  Key Features

* Data consolidation from multiple sources
* Schema standardization and data quality handling
* Scalable ETL pipeline using Spark
* Layered architecture for maintainability
* Real-world business use case simulation

---

## Use Case

This pipeline enables the retail company to:

* Get a unified view of customers and sales
* Analyze performance across merged entities
* Support decision-making with clean, reliable data

---


