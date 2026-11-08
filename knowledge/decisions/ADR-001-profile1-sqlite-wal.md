# ADR-001: Profile 1 Lean CDC Architecture with SQLite (WAL Mode)

- **Status:** Accepted (Approved by Human Architecture Review)
- **Date:** 2026-11-05
- **Deciders:** Principal Data Architect & Planner (`data-architecture-planner`), Human Engineer
- **Domain:** B2B Competitor Price & Market Intelligence (Tier 1 Fast Wedge)
- **Target Asset:** `b2b_price_monitor_v1` (`./contracts/b2b_price_monitor_v1.odcs.yaml`)
- **Institutional Memory:** `./knowledge/domain/semantic_rules.md`

---

## 1. Context & Problem Statement

The **B2B Price & Market Monitor** is a Tier 1 productized service targeting freelance contracts (Upwork, Workana) with tight delivery cycles (3 to 5 business days, ticket $180–350 USD / R$ 800–1,500) and ongoing retainer maintenance ($60 USD/mo / R$ 250/mo).

The core technical challenge is providing **Enterprise-grade reliability** (shift-left data quality, deterministic deduplication, zero false-positive alerts, auditable CDC ledger) without the massive operational overhead and idle resource footprint of distributed data lakehouse platforms (dbt Core + DuckDB/Iceberg or multi-container Airflow/Kafka/Postgres clusters).

### Pre-Flight Architectural Fitness Gate (0–6 Scorecard)
Prior to contract generation, the specification was evaluated against the 6 Medallion OLAP Fitness Criteria:
1. **Workload Pattern (`OLAP vs. OLTP/CDC`)**: **0/1 (FAIL)** — Requires Application-Level CDC, sub-millisecond point lookups (`WHERE entity_key = ?`), state hash diffing, and transactional outbox queues, conflicting with columnar OLAP batch aggregations.
2. **Latency SLA (`Scheduled DAG vs. Stream`)**: **1/1 (PASS)** — Scheduled Cron/daemon polling (15–60 min interval).
3. **Data Modality (`Tabular vs. Unstructured`)**: **1/1 (PASS)** — Structured tabular SKU price/stock attributes.
4. **Volume & Scale (`<100GB vs. Multi-TB`)**: **1/1 (PASS)** — 50 to 5,000 SKUs per cycle (<100 MB working set).
5. **Catalog Dependency (`Local vs. Live MCP`)**: **1/1 (PASS)** — Local `./contracts/` + git suffice.
6. **Lifecycle Scope (`Recurring vs. Disposable`)**: **1/1 (PASS)** — Recurring production monitoring asset.

**Score: 5/6 $\rightarrow$ Pre-Flight Circuit Breaker Triggered.**
The Human Engineer confirmed adapting the stack to **Profile 1 (Lean App/CDC)**.

---

## 2. Decision: Profile 1 Lean Architecture

We adopt **Profile 1: Lean App/CDC** using `Python + SQLite (WAL Mode) + Pydantic + Outbox Pattern`, while retaining strict ODCS v3.2.0 data contract governance and Open Knowledge Format (OKF) institutional memory.

### Core Architectural Pillars:

1. **Storage Engine — Embedded SQLite with Write-Ahead Logging (WAL)**:
   - File: `./b2b_monitor.db`
   - Config: `PRAGMA journal_mode = WAL;`, `PRAGMA synchronous = NORMAL;`, `PRAGMA busy_timeout = 5000;`.
   - Eliminates 100% of idle RAM consumption from client-server RDBMS; enables microsecond B-Tree index lookups for `entity_key` and non-blocking concurrent reads while writes commit.

2. **Dual-Hash Diff Engine (Zero False-Positive Spikes)**:
   - **Identity Hash (`entity_key`)**:
     $$\text{entity\_key} = \text{SHA-256}\left(\text{lower}(\text{trim}(\text{domain})) \parallel \text{'\|'} \parallel \text{lower}(\text{trim}(\text{canonical\_url\_or\_sku}))\right)$$
   - **State Signature (`state_hash`)**:
     $$\text{state\_hash} = \text{SHA-256}\left(\text{CAST}(\text{current\_price\_cents AS VARCHAR}) \parallel \text{'\|'} \parallel \text{upper}(\text{trim}(\text{currency\_code})) \parallel \text{'\|'} \parallel \text{upper}(\text{trim}(\text{availability\_status}))\right)$$
   - **Strict Exclusion**: Scrape timestamps (`last_checked_at`), page response latency, proxy IPs, user agents, and session cookies are strictly excluded from `state_hash`. Heartbeat runs update only `last_checked_at` without firing alerts.

3. **Strict Monetary Typing**:
   - Monetary values stored as `DECIMAL(18,4)` and mirrored in integer cents (`BIGINT`).
   - Floating-point representations (`FLOAT`, `DOUBLE`, `REAL`) are strictly banned to prevent IEEE 754 precision drift.

4. **Zero-Division & NULL Guard for Price Delta Percentage**:
   $$\text{price\_delta\_percent} = \begin{cases} 0.0000 & \text{if } \text{previous\_price IS NULL or } \text{previous\_price} = 0 \\ \frac{\text{current\_price} - \text{previous\_price}}{\text{previous\_price}} \times 100 & \text{otherwise} \end{cases}$$

5. **ACID Transactional Outbox State Machine**:
   - Notifications transition through `PENDING` $\rightarrow$ `DISPATCHED` or `FAILED` $\rightarrow$ `DEAD_LETTER`.
   - Physically backed by schema columns: `notification_status VARCHAR(32)`, `retry_count INTEGER DEFAULT 0`, and `last_notification_error VARCHAR(512)`.
   - Maximum 5 retry attempts with exponential backoff before terminal dead-lettering.

6. **Shift-Left Edge Validation & DLQ Isolation**:
   - Pydantic models validate payloads in memory before database touching.
   - Malformed HTML or template drift is diverted to `b2b_price_monitor_quarantine`.
   - If a domain's quarantine rate exceeds 20%, an automated domain-level Circuit Breaker halts alerts and notifies administrators of template drift.

---

## 3. Consequences & Trade-Offs

### Positive Consequences:
- **Zero Idle Overhead**: Runs comfortably in a single Docker container on a $5/mo VPS (1 vCPU, 1 GB RAM).
- **Zero Alert Noise**: The dual-hash model guarantees that cosmetic HTML alterations or repeated heartbeat runs never trigger false price-drop alerts.
- **Auditability**: Complete lineage, data contract conformance (ODCS v3.2.0), and reproducible state history.
- **Rapid Maintenance**: Clear separation of extraction logic from transactional state simplifies retaining clients on monthly SLA agreements.

### Accepted Trade-offs:
- **Single-Writer Concurrency**: SQLite locks the database file during writes. This is acceptable for single-node scrapers up to 5,000 SKUs/day.
- **No Direct Remote BI Connectivity**: External BI tools (Power BI, Metabase) cannot connect via network socket without exposing the SQLite file or exporting data.

---

## 4. Falsifiability Triggers (When to Migrate)

1. **Trigger A (Multi-Worker Concurrency or Remote BI)**: If concurrent scraper workers > 4 or real-time remote BI connections are mandated, migrate to **PostgreSQL 16** container with `COPY` / `ON CONFLICT` upserts.
2. **Trigger B (Enterprise Lakehouse Scale)**: If catalog scale exceeds 50,000 SKUs/day across > 10 domains with OLAP reporting requirements, migrate to **Profile 2 (Bruin + DuckDB/Iceberg)** or **Profile 3 (Snowflake/BigQuery)**.
