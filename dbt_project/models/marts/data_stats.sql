{{ config(materialized='view') }}

SELECT
    -- Count distinct values for key columns
    COUNT(DISTINCT PermitID) AS distinct_permitid,
    COUNT(DISTINCT PermitNum) AS distinct_permitnum,
    COUNT(DISTINCT MasterPermitNum) AS distinct_masterpermitnum,
    COUNT(DISTINCT OriginalAddress) AS distinct_address,
    COUNT(DISTINCT AppliedDate) AS distinct_applieddate,
    
    -- Total rows
    COUNT(*) AS total_rows,
    
    -- Percentage of NULLs
    SUM(CASE WHEN PermitID IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS permitid_null_pct,
    SUM(CASE WHEN PermitNum IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS permitnum_null_pct,
    SUM(CASE WHEN MasterPermitNum IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS masterpermitnum_null_pct,
    SUM(CASE WHEN OriginalAddress IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS address_null_pct,
    SUM(CASE WHEN AppliedDate IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS applieddate_null_pct,
    
    -- Duplication rate
    (COUNT(*) - COUNT(DISTINCT PermitID)) * 100.0 / COUNT(*) AS permitid_dup_pct,
    (COUNT(*) - COUNT(DISTINCT PermitNum)) * 100.0 / COUNT(*) AS permitnum_dup_pct,
    (COUNT(*) - COUNT(DISTINCT MasterPermitNum)) * 100.0 / COUNT(*) AS masterpermitnum_dup_pct,
    (COUNT(*) - COUNT(DISTINCT OriginalAddress)) * 100.0 / COUNT(*) AS address_dup_pct,
    (COUNT(*) - COUNT(DISTINCT AppliedDate)) * 100.0 / COUNT(*) AS applieddate_dup_pct
FROM {{ ref('filtered_permits') }}