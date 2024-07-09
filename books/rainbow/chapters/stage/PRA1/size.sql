SELECT
    SUM(PG_COLUMN_SIZE(x.*)) AS filesize
FROM
    "GFDL"."PRA1" AS x
WHERE
    x."BNS_DT" IN ({{ ID_LIST }});
