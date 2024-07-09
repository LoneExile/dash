SELECT
    SUM(PG_COLUMN_SIZE(x.*)) AS filesize
FROM
    "GFDL"."AMO0_KEYS" AS x
WHERE
    x."BNS_DT" IN ({{ ID_LIST }});
