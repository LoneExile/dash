CREATE TABLE "{{ BACKUP_TABLE_NAME }}" AS
SELECT
    *
FROM
    "ResultSegmentImpairment"
WHERE
    "CalculationId" IN ({{ ID_LIST }})
