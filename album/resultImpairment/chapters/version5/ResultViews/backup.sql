CREATE TABLE "{{ BACKUP_TABLE_NAME }}" AS
SELECT
    *
FROM
    "ResultViews"
WHERE
    "CalculationId" IN ({{ ID_LIST }})
