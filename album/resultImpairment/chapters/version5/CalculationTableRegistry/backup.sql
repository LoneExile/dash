CREATE TABLE "{{ BACKUP_TABLE_NAME }}" AS
SELECT
    *
FROM
    "CalculationTableRegistry"
WHERE
    "CalculationId" IN (
        SELECT
            "CalculationId" AS calculationid
        FROM
            "Calculations"
        WHERE
            "Data"::json ->> 'calculationSpecVersion' = '5');
