DELETE FROM "ResultSegmentImpairment"
WHERE "CalculationId" IN ({{ ID_LIST }})
