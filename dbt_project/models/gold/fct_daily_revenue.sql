{{
    config(
        materialized='table',
        alias='fct_daily_revenue'
    )
}}

-- Gold FinOps layer: Aggregates from Silver strictly following the canonical formula in semantic_rules.md
SELECT
    CAST(sha256(CAST(order_date AS VARCHAR) || '|' || currency_code || '|' || payment_method_type) AS CHAR(64)) AS revenue_grain_sk,
    order_date,
    currency_code,
    payment_method_type,
    COUNT(DISTINCT order_id) AS total_orders_count,
    CAST(SUM(gross_order_amount) AS DECIMAL(18,4)) AS gmv_amount,
    CAST(
        SUM(
            CASE
                WHEN order_status IN ('PAID', 'SHIPPED', 'DELIVERED', 'COMPLETED')
                 AND payment_status = 'CAPTURED'
                THEN net_order_amount
                ELSE CAST(0.0000 AS DECIMAL(18,4))
            END
        ) AS DECIMAL(18,4)
    ) AS recognized_net_revenue
FROM {{ ref('orders_silver') }}
GROUP BY
    order_date,
    currency_code,
    payment_method_type
