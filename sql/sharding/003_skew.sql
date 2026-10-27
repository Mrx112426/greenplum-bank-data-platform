-- {table} is an allowlisted table, substituted by the runner.
-- For replicated tables the runner counts physical copies via gp_dist_random/gp_execution_segment.
WITH counts AS (
 SELECT gp_segment_id AS content,count(*) AS rows FROM {table} GROUP BY gp_segment_id
)
SELECT s.content,coalesce(c.rows,0) AS rows
FROM gp_segment_configuration s LEFT JOIN counts c USING (content)
WHERE s.content>=0 AND s.role='p' AND s.status='u' ORDER BY s.content
