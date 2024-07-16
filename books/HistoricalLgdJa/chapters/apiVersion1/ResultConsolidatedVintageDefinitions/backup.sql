CREATE TABLE "{{ BACKUP_TABLE_NAME }}" AS
SELECT
    *
FROM
    "{{ CURRENT_DIR }}"
WHERE
    "CalculationId" IN ({{ ID_LIST }})
