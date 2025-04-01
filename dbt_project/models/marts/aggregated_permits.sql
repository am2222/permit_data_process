{{ config(materialized='table') }}

SELECT
    EXTRACT(YEAR FROM CAST(IssuedDate AS TIMESTAMP)) AS issue_year,
    PermitType,
    COUNT(*) AS permit_count
FROM {{ ref('deduplicated_permits') }}
GROUP BY 
    EXTRACT(YEAR FROM CAST(IssuedDate AS TIMESTAMP)),
    PermitType