{{ config(materialized='table') }}

SELECT *
FROM {{ ref('construction_permits') }}