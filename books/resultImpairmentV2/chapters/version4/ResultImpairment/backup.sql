CREATE TABLE "{{ BACKUP_TABLE_NAME }}" AS
SELECT
    *
FROM
    "ResultImpairment"
WHERE
    "CalculationId" IN ({{ ID_LIST }})
