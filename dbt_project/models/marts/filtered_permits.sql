{{ config(materialized='table') }}

SELECT *
FROM {{ ref('stg_permits') }}
WHERE CAST(IssuedDate AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '{{ var("years", 5) }} years'