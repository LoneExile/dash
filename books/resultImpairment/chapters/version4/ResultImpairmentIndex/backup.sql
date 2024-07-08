CREATE TABLE "{{ BACKUP_TABLE_NAME }}" AS
SELECT
    *
FROM
    "ResultImpairmentIndex"
WHERE
    "CalculationId" IN ({{ ID_LIST }})
