SELECT
    cal.calculationId
FROM (
    SELECT
        c."CalculationId" AS calculationId,
        c."Data"::json ->> 'calculationSpecVersion' AS version,
        c."CreatedWhen"
    FROM
        "Calculations" c) AS cal
WHERE
    cal.version = '5';
