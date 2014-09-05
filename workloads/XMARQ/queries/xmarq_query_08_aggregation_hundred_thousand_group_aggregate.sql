SELECT
 SUBSTRING(o_comment, 1, 15) AS substring_comment,
 COUNT(*) AS num
FROM
 orders_TABLESUFFIX
GROUP BY
 substring_comment;
