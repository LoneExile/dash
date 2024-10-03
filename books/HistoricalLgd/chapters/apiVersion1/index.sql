SELECT
    calculationid,
    createdwhen
FROM (
    SELECT
        c."CalculationId" AS calculationid,
        c."CreatedWhen" AS createdwhen,
        c."Data"::json ->> 'calculationSpecVersion' AS version
    FROM
        "Calculations" AS c
    {{ CUSTOM_WHERE }}) AS calculations
WHERE
    version = '1'
    {{ IS_WHERE_DATE }} AND "createdwhen" BETWEEN '{{ START_DATE }}' AND '{{ END_DATE }}'
