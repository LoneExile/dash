CREATE TABLE "{{ BACKUP_TABLE_NAME }}" AS
SELECT
    *
FROM
    "CalculationMetrics"
WHERE
    "CalculationId" IN ({{ ID_LIST }})
