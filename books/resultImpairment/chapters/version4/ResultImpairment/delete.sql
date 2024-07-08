DELETE FROM "ResultImpairment"
WHERE "CalculationId" IN ({{ ID_LIST }})
