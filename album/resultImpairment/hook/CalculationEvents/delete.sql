DELETE FROM "CalculationEvents"
WHERE "CalculationId" IN ({{ ID_LIST }})
