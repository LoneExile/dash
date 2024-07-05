DELETE FROM "CalculationTableRegistry"
WHERE "CalculationId" IN ({{ ID_LIST }})
