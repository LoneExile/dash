DELETE FROM "CalculationMetrics"
WHERE "CalculationId" IN ({{ ID_LIST }})
