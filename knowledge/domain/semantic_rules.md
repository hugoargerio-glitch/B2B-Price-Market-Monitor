---
okf_version: "1.0"
document_id: "domain-semantic-rules-b2b-price-monitor-v1"
domain: "competitor_intelligence"
owners: ["data-architecture-team", "b2b-monitoring-ops"]
linked_contracts:
  - "./contracts/b2b_price_monitor_v1.odcs.yaml"
linked_adrs:
  - "./knowledge/decisions/ADR-001-profile1-sqlite-wal.md"
last_verified: "2026-11-05"
---

# Canonical Domain Semantic Rules — B2B Price & Market Monitor (`semantic_rules.md`)

This **Open Knowledge Format (OKF)** document defines the institutional single source of truth for the **B2B Price & Market Monitor** (Profile 1: Lean App/CDC). Every subagent (`planner`, `worker`, `auditor`, `dataops`) must consult this file before proposing, implementing, or auditing pipeline and data contract logic.

---

## 1. Entity Definitions & Storage Architecture (Profile 1)

| Table | Storage Engine | Atomic Grain | Primary Key | State / Retention Model |
| :--- | :--- | :--- | :--- | :--- |
| `b2b_price_monitor` | SQLite (WAL Mode) | **1 row per monitored product entity** | `entity_key` | Current State (SCD Type 1) + Outbox Queue |
| `b2b_price_monitor_quarantine` | SQLite (WAL Mode) | 1 row per failed extraction / validation | `quarantine_id` | Dead Letter Queue (DLQ) Append-Only |

### Storage Engine Constraints
- **MANDATE**: Operate on embedded SQLite (`database: ./b2b_monitor.db`) running with `PRAGMA journal_mode = WAL`, `PRAGMA synchronous = NORMAL`, and `PRAGMA busy_timeout = 5000` to support non-blocking concurrent reads and resilient single-node execution.
- **!ANTI_PATTERN**: Never run SQLite in default `DELETE` rollback journal mode, as concurrent reads during scrape ingestion will raise `database is locked` exceptions.

---

## 2. Dual-Hash Diff Engine & Zero False-Positive Guarantees

### 2.1. Identity Key vs. State Signature
- **MANDATE**: Compute `entity_key` as `SHA-256(lower(trim(domain)) || '|' || lower(trim(canonical_url_or_sku)))` and index it via a unique B-Tree index to guarantee sub-millisecond point lookups (`SELECT state_hash, current_price_cents FROM b2b_price_monitor WHERE entity_key = ?`).
- **!ANTI_PATTERN**: Never use raw, un-normalized URLs or mutable catalog titles as primary keys, as trailing slashes, URL tracking parameters (`utm_*`, `ref`), and copywriting adjustments create orphan entity duplicates.
- **MANDATE**: Compute `state_hash` strictly and exclusively from business state attributes:
  $$\text{state\_hash} = \text{SHA-256}\left(\text{CAST}(\text{current\_price\_cents AS VARCHAR}) \parallel \text{'\|'} \parallel \text{upper}(\text{trim}(\text{currency\_code})) \parallel \text{'\|'} \parallel \text{upper}(\text{trim}(\text{availability\_status}))\right)$$
- **!ANTI_PATTERN**: Never include transient metadata (scrape timestamps like `last_checked_at`, request latencies, proxy IP addresses, or HTTP session cookies) inside the `state_hash` formula, which would invalidate state parity on subsequent runs and trigger false-positive alert floods.

### 2.2. State Transition & Heartbeat Logic
- **MANDATE**: Upon each scrape run, execute the deterministic state diff:
  - **No State Change (`new_state_hash == existing_state_hash`)**: Update **only** `last_checked_at = CURRENT_TIMESTAMP` (Heartbeat). Do **not** mutate `last_changed_at`, do **not** touch `previous_price`, and do **not** trigger webhook dispatch.
  - **State Transition (`new_state_hash != existing_state_hash`)**: Atomically update:
    - `previous_price = current_price`
    - `current_price = new_price`
    - `current_price_cents = new_price_cents`
    - `price_delta = new_price - current_price`
    - `price_delta_percent = CASE WHEN current_price IS NULL OR current_price = 0 THEN 0.0000 ELSE CAST(((new_price - current_price) / current_price) * 100.0000 AS DECIMAL(18,4)) END`
    - `state_hash = new_state_hash`
    - `last_changed_at = CURRENT_TIMESTAMP`
    - `last_checked_at = CURRENT_TIMESTAMP`
    - `notification_status = 'PENDING'`
    - `retry_count = 0`
    - `last_notification_error = NULL`
- **!ANTI_PATTERN**: Never overwrite `previous_price` or reset `notification_status` during routine heartbeat runs where `new_state_hash == existing_state_hash`.

---

## 3. Monetary Precision & Calculation Formulas

### 3.1. Typing & Arithmetic Constraints
- **MANDATE**: Store all monetary values as `DECIMAL(18,4)` and mirror them in integer cents (`BIGINT`) for exact arithmetic.
- **!ANTI_PATTERN**: Never use floating-point primitives (`FLOAT`, `DOUBLE`, `REAL`) for unit prices or deltas; all comparison and diff logic must operate on integer cents (`current_price_cents`) or fixed-point decimals.
- **MANDATE**: Ensure that `CAST(ROUND(current_price * 100, 0) AS BIGINT) == current_price_cents` holds invariant across 100% of rows.
- **!ANTI_PATTERN**: Never allow negative values in `current_price` or `current_price_cents`.

### 3.2. Verified Metric Equations
1. **Absolute Price Delta (`price_delta`)**:
   $$\text{price\_delta} = \text{current\_price} - \text{COALESCE}(\text{previous\_price}, \text{current\_price})$$

2. **Relative Price Delta Percentage (`price_delta_percent`) with Zero-Division Guard**:
   $$\text{price\_delta\_percent} = \begin{cases} 0.0000 & \text{if } \text{previous\_price IS NULL or } \text{previous\_price} = 0 \\ \text{CAST}\left(\frac{\text{current\_price} - \text{previous\_price}}{\text{previous\_price}} \times 100.0000 \text{ AS DECIMAL(18,4)}\right) & \text{otherwise} \end{cases}$$
   - **SQL Reference Implementation**:
     ```sql
     CASE
       WHEN previous_price IS NULL OR previous_price = 0
       THEN CAST(0.0000 AS DECIMAL(18,4))
       ELSE CAST(((current_price - previous_price) / previous_price) * 100.0000 AS DECIMAL(18,4))
     END AS price_delta_percent
     ```

---

## 4. ACID Transactional Outbox State Machine (5-Attempt Queue)

- **MANDATE**: Manage notification dispatch via a strict 4-state finite state machine physically backed by schema columns (`notification_status`, `retry_count`, `last_notification_error`):
  1. `PENDING`: Initial state set atomically by the Diff Engine when `new_state_hash != existing_state_hash`.
  2. `DISPATCHED`: Final success state stamped when the target webhook endpoint (Discord, Slack, MS Teams) returns HTTP 200/204.
  3. `FAILED`: Intermediate retry state stamped when webhook dispatch encounters a transient failure (HTTP 429 rate limit, HTTP 5xx, or network timeout).
     - Each failed attempt increments `retry_count = retry_count + 1` and records the error snippet in `last_notification_error`.
     - Retries must execute with exponential backoff ($1\text{s}, 2\text{s}, 4\text{s}, 8\text{s}, 16\text{s}$).
  4. `DEAD_LETTER`: Terminal failure state stamped when `retry_count >= 5`. Emits an administrative alert and halts retries for that event.
- **!ANTI_PATTERN**: Never perform synchronous webhook dispatch within the scraper loop; notification dispatch must be decoupled through the Outbox queue to prevent scraper blocking or state corruption on webhook failures.
- **!ANTI_PATTERN**: Never leave a failed notification in `PENDING` without recording retry counters or transition to `FAILED` / `DEAD_LETTER`.

---

## 5. Shift-Left Edge Validation & Quarantine Isolation

- **MANDATE**: Validate all extracted scrape payloads in-memory against the Pydantic schema before invoking the Diff Engine. Any missing mandatory selectors (price, availability) or parsing exceptions must be diverted immediately to `b2b_price_monitor_quarantine` with `quarantine_id = UUIDv7`, `raw_url`, `raw_payload`, and diagnostic `failure_reason`.
- **!ANTI_PATTERN**: Never write raw unvalidated HTML snippets, `NULL` prices, or zero-price fallbacks directly into `b2b_price_monitor`, as this corrupts the baseline state and triggers erroneous price-drop alerts.
- **MANDATE**: Enforce a domain-level Circuit Breaker: if the scrape failure/quarantine rate for a domain exceeds 20% of its target catalog in a single run, halt webhook alerting for that domain and trigger a high-priority `TEMPLATE_DRIFT_DETECTED` administrative notification.
- **!ANTI_PATTERN**: Never allow automated crawlers to continue sending customer-facing alerts when template drift is detected on the target site.
