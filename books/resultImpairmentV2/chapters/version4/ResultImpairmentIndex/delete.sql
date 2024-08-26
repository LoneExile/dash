DELETE FROM "ResultImpairmentIndex"
WHERE "CalculationId" IN ({{ ID_LIST }})
