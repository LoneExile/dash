DELETE FROM "CombinedCalculations"
WHERE "CalculationId" IN ({{ ID_LIST }})
