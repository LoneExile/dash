CREATE TABLE "{{ BACKUP_TABLE_NAME }}" AS
SELECT
    *
FROM
    "CombinedCalculations" AS ce
WHERE
    ce."CalculationId" IN ({{ ID_LIST }})
