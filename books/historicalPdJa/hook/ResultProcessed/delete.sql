DELETE FROM "ResultProcessed"
WHERE "CalculationId" IN ({{ ID_LIST }})
