-- The aggregate fingerprint is order-independent and independent of placement.
-- Two signed 64-bit halves of each row's MD5 are summed with NUMERIC precision.
SELECT count(*) AS rows,
coalesce(sum(('x'||substr(md5(ROW({columns})::text),1,16))::bit(64)::bigint),0)::text AS hash_sum_1,
coalesce(sum(('x'||substr(md5(ROW({columns})::text),17,16))::bit(64)::bigint),0)::text AS hash_sum_2
FROM {table}
