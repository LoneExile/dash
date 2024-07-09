SELECT
    calculationid
FROM (
    SELECT
        c."CalculationId" AS calculationid,
        c."CreatedWhen",
        c."Data"::json ->> 'calculationSpecVersion' AS version
    FROM
        "Calculations" AS c) as calculations
WHERE
    version = '2'
    {{ IS_WHERE_DATE }} AND "CreatedWhen" BETWEEN '{{ START_DATE }}' AND '{{ END_DATE }}'
