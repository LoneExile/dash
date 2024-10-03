DELETE FROM "{{ CURRENT_DIR }}"
WHERE
    "CalculationId" IN ({{ ID_LIST }})
