CREATE TABLE "{{ BACKUP_TABLE_NAME }}" AS
SELECT
    *
FROM
    "CalculationTableRegistry"
WHERE
    "CalculationId" IN ({{ ID_LIST }})
