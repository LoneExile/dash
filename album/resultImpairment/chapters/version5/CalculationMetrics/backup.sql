CREATE TABLE "{{ BACKUP_TABLE_NAME }}" AS
SELECT
    *
FROM
    "CalculationMetrics"
WHERE
    "CalculationId" IN (
        SELECT
            "CalculationId" AS calculationid
        FROM
            "Calculations"
        WHERE
            "Data"::json ->> 'calculationSpecVersion' = '5');

-- create table {{ BACKUP_TABLE_NAME }} as
-- select *
-- from "CalculationMetrics"
-- where "CalculationId" in ( {{list_id}} )
