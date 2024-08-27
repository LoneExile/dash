SELECT
    calculationid,
    createdwhen
FROM (
    SELECT
        c."CalculationId" AS calculationid,
        c."CreatedWhen" AS createdwhen,
        c."Data"::json ->> 'calculationSpecVersion' AS version
    FROM
        "Calculations" AS c) as cal
WHERE
    version = '5'
    {{ IS_WHERE_DATE }} AND "createdwhen" BETWEEN '{{ START_DATE }}' AND '{{ END_DATE }}'
