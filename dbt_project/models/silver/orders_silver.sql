{{
    config(
        materialized='table',
        alias='orders_silver'
    )
}}

WITH deduplicated_cdc AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_updated_at DESC, event_id DESC
        ) AS _cdc_rn
    FROM {{ source('bronze_ecommerce', 'orders_raw') }}
),

cleaned_and_reconciled AS (
    SELECT
        CAST(order_id AS VARCHAR(64)) AS order_id,
        CAST(customer_id AS VARCHAR(64)) AS customer_id,
        -- Salted SHA-256 PII anonymization (MANDATE rule from semantic_rules.md)
        CAST(sha256(lower(trim(customer_email)) || '{{ var("pii_hash_salt") }}') AS CHAR(64)) AS customer_email_hash,
        CAST(sha256(lower(trim(customer_tax_id)) || '{{ var("pii_hash_salt") }}') AS CHAR(64)) AS customer_tax_id_hash,
        CAST(order_status AS VARCHAR(32)) AS order_status,
        CAST(payment_status AS VARCHAR(32)) AS payment_status,
        CAST(payment_method_type AS VARCHAR(32)) AS payment_method_type,
        CAST(currency_code AS CHAR(3)) AS currency_code,
        CAST(gross_order_amount_cents AS BIGINT) AS gross_order_amount_cents,
        CAST(COALESCE(gross_order_amount, 0.0000) AS DECIMAL(18,4)) AS gross_order_amount,
        CAST(COALESCE(discount_amount, 0.0000) AS DECIMAL(18,4)) AS discount_amount,
        CAST(COALESCE(tax_amount, 0.0000) AS DECIMAL(18,4)) AS tax_amount,
        CAST(COALESCE(shipping_amount, 0.0000) AS DECIMAL(18,4)) AS shipping_amount,
        -- Verified accounting identity: net = gross - discount + tax + shipping
        CAST(
            COALESCE(gross_order_amount, 0.0000)
            - COALESCE(discount_amount, 0.0000)
            + COALESCE(tax_amount, 0.0000)
            + COALESCE(shipping_amount, 0.0000)
            AS DECIMAL(18,4)
        ) AS net_order_amount,
        CAST(order_created_at AS TIMESTAMPTZ) AS order_created_at,
        CAST(order_updated_at AS TIMESTAMPTZ) AS order_updated_at,
        -- Accounting partition date in America/Sao_Paulo timezone
        CAST(order_created_at AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS order_date,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMPTZ) AS _ingested_at
    FROM deduplicated_cdc
    WHERE _cdc_rn = 1
)

SELECT *
FROM cleaned_and_reconciled
