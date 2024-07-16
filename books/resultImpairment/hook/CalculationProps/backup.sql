CREATE TABLE "{{ BACKUP_TABLE_NAME }}" AS
SELECT
    *
FROM
    "{{ CURRENT_DIR }}" AS ce
WHERE
    ce."CalculationId" IN ({{ ID_LIST }})
