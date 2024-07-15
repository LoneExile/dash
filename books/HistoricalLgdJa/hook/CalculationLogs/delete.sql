DELETE FROM "CalculationLogs"
WHERE "CalculationId" IN ({{ ID_LIST }})
