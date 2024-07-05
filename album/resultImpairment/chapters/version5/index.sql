SELECT
    calculationid
FROM (
    SELECT
        c."CalculationId" AS calculationid,
        c."CreatedWhen",
        c."Data"::json ->> 'calculationSpecVersion' AS version
    FROM
        "Calculations" AS c)
WHERE
    version = '5'
    {{ IS_WHERE_CLAUSE_DATE }} AND "CreatedWhen" BETWEEN '{{ START_DATE }}' AND '{{ END_DATE }}'

    -- AND "" BETWEEN '2024-01-01 03:14:57' AND '2024-12-16 03:14:57'
