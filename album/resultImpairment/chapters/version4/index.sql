SELECT
    cal.calculationid
FROM (
    SELECT
        c."CalculationId" AS calculationid,
        c."CreatedWhen",
        c."Data"::json ->> 'calculationSpecVersion' AS version
    FROM
        "Calculations" AS c) AS cal
WHERE
    cal.version = '4'
    {{ IS_WHERE_CLAUSE_DATE }} AND "CreatedWhen" BETWEEN '{{ START_DATE }}' AND '{{ END_DATE }}'
