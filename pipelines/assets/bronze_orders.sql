/* @bruin
name: bronze_ecommerce.orders_raw
type: duckdb.sql
connection: duckdb-default
description: |
  Bronze layer asset governed by Bruin containing raw e-commerce order CDC events.
  Intentionally includes multiple CDC events for the same order_id (to verify Silver-layer ROW_NUMBER deduplication)
  and cleartext PII fields (customer_email, customer_tax_id) that are hashed with salted SHA-256 by dbt in Silver.
materialization:
  type: table

columns:
  - name: event_id
    type: VARCHAR
    description: Unique identifier of the raw CDC event
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: order_id
    type: VARCHAR
    description: Natural order identifier
    checks:
      - name: not_null
  - name: customer_id
    type: VARCHAR
    description: Customer account identifier
    checks:
      - name: not_null
@bruin */

SELECT *
FROM (
    VALUES
        -- Order 1 (Initial PENDING_PAYMENT CDC state -> superseded by newer CDC event below)
        (
            'EVT-001',
            '018f4a9c-7d2b-7000-8a11-3c9e5b1029a1',
            'CUST-9948101',
            'ana.silva@exemplo.com.br',
            '123.456.789-00',
            'PENDING_PAYMENT',
            'PENDING',
            'PIX',
            'BRL',
            CAST(25000 AS BIGINT),
            CAST(250.0000 AS DECIMAL(18,4)),
            CAST(20.0000 AS DECIMAL(18,4)),
            CAST(15.0000 AS DECIMAL(18,4)),
            CAST(10.0000 AS DECIMAL(18,4)),
            TIMESTAMPTZ '2026-11-04 12:00:00Z',
            TIMESTAMPTZ '2026-11-04 12:00:00Z'
        ),
        -- Order 1 (Updated PAID / CAPTURED state -> wins in Silver ROW_NUMBER deduplication)
        (
            'EVT-002',
            '018f4a9c-7d2b-7000-8a11-3c9e5b1029a1',
            'CUST-9948101',
            'Ana.Silva@Exemplo.com.br ',
            '123.456.789-00',
            'PAID',
            'CAPTURED',
            'PIX',
            'BRL',
            CAST(25000 AS BIGINT),
            CAST(250.0000 AS DECIMAL(18,4)),
            CAST(20.0000 AS DECIMAL(18,4)),
            CAST(15.0000 AS DECIMAL(18,4)),
            CAST(10.0000 AS DECIMAL(18,4)),
            TIMESTAMPTZ '2026-11-04 12:00:00Z',
            TIMESTAMPTZ '2026-11-04 12:05:00Z'
        ),
        -- Order 2 (Delivered Credit Card order)
        (
            'EVT-003',
            '018f4a9c-8e10-7000-9b22-4d8f6c2038b2',
            'CUST-9948102',
            'carlos.souza@empresa.io',
            '98.765.432/0001-10',
            'DELIVERED',
            'CAPTURED',
            'CREDIT_CARD',
            'BRL',
            CAST(89990 AS BIGINT),
            CAST(899.9000 AS DECIMAL(18,4)),
            CAST(50.0000 AS DECIMAL(18,4)),
            CAST(80.9900 AS DECIMAL(18,4)),
            CAST(25.0000 AS DECIMAL(18,4)),
            TIMESTAMPTZ '2026-11-04 14:30:00Z',
            TIMESTAMPTZ '2026-11-04 16:00:00Z'
        ),
        -- Order 3 (Cancelled Boleto order -> included in GMV, excluded from Official Recognized Net Revenue)
        (
            'EVT-004',
            '018f4a9c-9f99-7000-ac33-5e7a7d3047c3',
            'CUST-9948103',
            'mariana.costa@email.org',
            '456.789.123-99',
            'CANCELLED',
            'REFUNDED',
            'BOLETO',
            'BRL',
            CAST(15000 AS BIGINT),
            CAST(150.0000 AS DECIMAL(18,4)),
            CAST(0.0000 AS DECIMAL(18,4)),
            CAST(12.0000 AS DECIMAL(18,4)),
            CAST(15.0000 AS DECIMAL(18,4)),
            TIMESTAMPTZ '2026-11-04 15:10:00Z',
            TIMESTAMPTZ '2026-11-04 17:00:00Z'
        )
) AS raw_cdc (
    event_id,
    order_id,
    customer_id,
    customer_email,
    customer_tax_id,
    order_status,
    payment_status,
    payment_method_type,
    currency_code,
    gross_order_amount_cents,
    gross_order_amount,
    discount_amount,
    tax_amount,
    shipping_amount,
    order_created_at,
    order_updated_at
);
