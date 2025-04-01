{{ config(materialized='table') }}

SELECT
    COBPIN AS UniqueParcelIdentificationNumber,
    BOCOPIN AS AssessorParcelIdentificationNumber,
    BOCOTAX AS AssessorTaxAccountNumber,
    *
FROM {{ ref('deduplicated_permits') }}