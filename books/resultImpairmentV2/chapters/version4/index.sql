SELECT
    cal.calculationid,
    cal.createdwhen
FROM (
    SELECT
        c."CalculationId" AS calculationid,
        c."CreatedWhen" AS createdwhen,
        c."Data"::json ->> 'calculationSpecVersion' AS version
    FROM
        "Calculations" AS c) AS cal
WHERE
    cal.version = '4'
    {{ IS_WHERE_DATE }} AND "CreatedWhen" BETWEEN '{{ START_DATE }}' AND '{{ END_DATE }}'
