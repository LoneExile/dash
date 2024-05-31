SELECT
    COUNT(*) AS total_count
FROM
    "ResultImpairment"
WHERE
    "CalculationId" = '{{ ID }}';
