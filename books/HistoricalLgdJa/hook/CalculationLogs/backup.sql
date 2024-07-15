CREATE TABLE "{{ BACKUP_TABLE_NAME }}" AS
SELECT
    *
FROM
    "CalculationLogs" AS ce
WHERE
    ce."CalculationId" IN ({{ ID_LIST }})
