SET SEARCH_PATH TO mk_3623_1;

DROP TABLE IF EXISTS tmp_realtime;
CREATE TEMP TABLE tmp_realtime AS
SELECT
  l.source_key_value,
  NULLIF(JSON_EXTRACT_PATH_TEXT(l.payload, 'mk_PR_fit_segment__c'), '') AS mk_PR_fit_segment__c,
  ROW_NUMBER() OVER (PARTITION BY l.source_key_value ORDER BY push_timestamp ASC NULLS LAST) AS rk
FROM
WHERE
  l.source_system = 'salesforce'
  AND l.source_system_object = 'Lead'
  AND push_timestamp > GETDATE() - 7
  AND NULLIF(JSON_EXTRACT_PATH_TEXT(l.payload, 'mk_PR_fit_segment__c'), '') IS NOT NULL
;

DELETE FROM tmp_realtime WHERE rk > 1;

SELECT
  COUNT(1)
FROM
  tmp_realtime r
LEFT JOIN
  prototype_pr_lead_grade cf
  ON r.source_key_value = cf.source_key_value
  AND cf.source_system = 'salesforce'
  AND cf.source_system_object = 'Lead'
WHERE
  r.mk_PR_fit_segment__c = cf.customer_fit
;