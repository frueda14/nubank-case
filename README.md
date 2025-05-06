# Nubank – Analytics Engineer Case

This repository contains the solution to Nubank’s Analytics Engineer case challenge.  
The objective is to build a reliable, production-ready pipeline that delivers the **monthly closing balance per customer account**, using raw CSV files as input.

---

## 🧠 Business context

Understanding how much money customers keep in their accounts and how that changes over time is key for:

- Segmenting high value or at risk customers  
- Powering growth, retention, and risk models  
- Reconciling financial data across teams

---

## 🏗️ Architecture

CSV files
↓
RAW (transient) — direct copy from internal stage
↓
STAGING (views) — typecasting, renaming
↓
INTERMEDIATE (tables) — business logic: signed transactions
↓
MART (incremental) — monthly balance per account


> Built with **Snowflake** (transient DB, XS warehouse) and **dbt** (SQL-based modeling, testing, and documentation).

---

## 🚀 Tech stack

- **Snowflake** – cloud data warehouse, transient + cost-controlled  
- **dbt-core** – transformations, tests, lineage  
- **SnowSQL CLI** – staging and loading files  

---

## 📁 Structure

models/ → dbt models (staging, intermediate, marts)
scripts/ddl/ → SQL setup (DB, schema, stage)
tests/ → custom SQL tests
.env.example → Snowflake config template
README.md → this file


---

## 🧩 Problem 2 – Suggested Improvements

- Add freshness + reconciliation tests  
- Optimize with incremental + clustered tables  
- Create new KPIs (margin, dormancy, volatility)  
- Add orchestration, CI, and documentation pipeline

---

## 👤 Author

**frueda14**