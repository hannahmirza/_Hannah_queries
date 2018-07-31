-- Iterating on trees for CFit

-- QAing tree nodes 
SELECT
  domain,
  email,
  target,
  mk_job_title,
  predicted_revenue_segment,
  predicted_revenue_segment_hg,
  predicted_traffic,
  tag_is_b2c,
  tag_is_b2b_saas,
  tag_is_entb2b

FROM analysis_observations_fit_for_training
-- Node conditions from terminal
WHERE (COALESCE(company__geo__gdp_per_capita,'unknown') = 'high') AND (COALESCE(mk_job_title, 'N/A') = 'N/A') AND  NOT (COALESCE(predicted_revenue,0) >= 20000000) AND  NOT (COALESCE(predicted_traffic,0) >= 500000) AND  NOT (COALESCE(predicted_revenue,0) > 2000000) AND  NOT (COALESCE(has_ecom_tech, 0) = 1)
ORDER BY target DESC
LIMIT 200
;

-- univariate on a node
SELECT
is_edu,
 COUNT(1),
  SUM(target),
1.0 * SUM(target)/COUNT(1) AS conv_rate
FROM analysis_observations_fit_for_training
-- node conditions in terminal
WHERE (COALESCE(company__geo__gdp_per_capita,'unknown') = 'high') AND (COALESCE(mk_job_title, 'N/A') = 'N/A') AND  NOT (COALESCE(predicted_revenue,0) >= 20000000) AND  NOT (COALESCE(predicted_traffic,0) >= 500000) AND  NOT (COALESCE(predicted_revenue,0) > 2000000) AND  NOT (COALESCE(has_ecom_tech, 0) = 1)
GROUP BY 1

;


-- Generator Code:

SELECT
      ROUND(logit_segment, 1) AS logit,
      count(DISTINCT CASE WHEN a.id IS NOT NULL THEN c.email ELSE NULL END) AS "Conversion",
      count(distinct c.email) AS "Population"
--       100.0 * count(DISTINCT CASE WHEN a.id IS NOT NULL THEN c.email ELSE NULL END) / count(distinct c.email) AS "Conversion Rate",
--       avg(a.amount) as avg_amount
--       , MIN(logit_segment) AS min_logit,
--       MAX(logit_segment) AS max_logit
FROM
      tmp_audience AS c
INNER JOIN
      analysis_scoring_customer_fit_new AS af
      ON c.source_system = af.source_system
      AND c.source_system_object = af.source_system_object
      AND c.source_key_name = af.source_key_name
      AND c.source_key_value = af.source_key_value
LEFT JOIN
    tmp_target AS a
    ON c.accountid = a.accountid
    AND c.created_date <= a.created_date
GROUP BY
    1
ORDER BY 1 ASC