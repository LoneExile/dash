COPY (
    SELECT
        *
    FROM
        "ApplicationLogs"
    WHERE
        "LogId" = '738843e8')
TO STDOUT WITH CSV HEADER;
