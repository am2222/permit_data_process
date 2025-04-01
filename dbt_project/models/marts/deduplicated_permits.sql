{{ config(materialized='table') }}

WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY {{ var('dup_cols', 'IssuedDate, ProjectName, Description, OriginalAddress') }}
            ORDER BY ObjectId
        ) AS rn
    FROM {{ ref('filtered_permits') }}
)
SELECT *
FROM deduped
WHERE rn = 1