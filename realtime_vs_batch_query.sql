SET SEARCH_PATH TO mk_3764_1;

DROP TABLE IF EXISTS tmp_realtime;
CREATE TEMP TABLE tmp_realtime AS
SELECT
  l.source_key_value,
  JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(JSON_EXTRACT_PATH_TEXT(payload, 'properties'), 9), 'value') AS customer_fit,
  ROW_NUMBER() OVER (PARTITION BY l.source_key_value ORDER BY push_timestamp DESC NULLS LAST) AS rk
FROM
  export_contacts_log l
WHERE
  l.source_system = 'hubspot'
  AND push_timestamp > GETDATE() - 3
  AND payload ~ 'mk_customer_fit_segment'
  -- make sure it's realtime (only push lead_grade in realtime)
  AND payload ~ 'mk_lead_grade_score'
;

DELETE FROM tmp_realtime WHERE rk > 1;

SELECT
--   COUNT(DISTINCT cf.email) AS count_all,
--   COUNT(DISTINCT CASE WHEN r.customer_fit = cf.customer_fit THEN cf.email END) AS cnt_same,
--   COUNT(DISTINCT CASE WHEN r.customer_fit <> cf.customer_fit THEN cf.email END) AS cnt_diff
  cf.email,
  r.source_key_value,
  REPLACE(r.customer_fit, '_', ' ') AS realtime_cfit,
  REPLACE(cf.customer_fit, '_', ' ') AS batch_cfit
FROM
  tmp_realtime r
INNER JOIN
  analysis_scoring_customer_fit cf
  ON r.source_key_value = cf.source_key_value
WHERE
  REPLACE(r.customer_fit, '_', ' ') <> REPLACE(cf.customer_fit, '_', ' ')
LIMIT 100
;