
-- feature discovery on a node
SELECT
  target,
  employees,
  predicted_traffic,
  alexa_global,
  
FROM
  analysis_observations_fit_for_training
WHERE
  -- put node code here
  (COALESCE(is_personal,0) = 0 AND COALESCE(is_spam,0) = 0 AND COALESCE(is_student,0) = 0 AND COALESCE(is_edu,0) = 0 AND COALESCE(exclusions,0) = 0)
      AND (COALESCE(employees,-333) > 0) AND (COALESCE(employees,-333) < 100) AND (COALESCE(employees,-333) >= 10) AND (COALESCE(employees,-333) <= 50)
      AND (COALESCE(industry, 'unknown') IN ('Internet Software & Services', 'Professional Services'))
      AND  NOT (COALESCE(alexa_global, 0) > 4000000)
      AND  NOT (COALESCE(predicted_revenue_segment, 'unknown') = '1- less than $1m') AND (COALESCE(predicted_traffic, 0) > 25000)
ORDER BY 
  target DESC
LIMIT 200
;

-- univariate on one feature on a node
SELECT
  tech_cnt_bucket,
  COUNT(1),
  SUM(target),
  SUM(target)::FLOAT / COUNT(1)::FLOAT AS conversion_rate

FROM
  analysis_observations_fit_for_training
WHERE
  -- put node code here
 (COALESCE(is_personal,0) = 0 AND COALESCE(is_spam,0) = 0 AND COALESCE(is_student,0) = 0 AND COALESCE(is_edu,0) = 0 AND COALESCE(exclusions,0) = 0)
      AND (COALESCE(employees,-333) > 0) AND (COALESCE(employees,-333) < 100) AND (COALESCE(employees,-333) >= 10) AND (COALESCE(employees,-333) <= 50)
      AND (COALESCE(industry, 'unknown') IN ('Internet Software & Services', 'Professional Services'))
      AND  NOT (COALESCE(alexa_global, 0) > 4000000)
      AND  NOT (COALESCE(predicted_revenue_segment, 'unknown') = '1- less than $1m') AND (COALESCE(predicted_traffic, 0) > 25000)
GROUP BY 1
ORDER BY
  2 DESC
LIMIT 200
;


-- measure performance of new node(s)
SELECT
  COUNT(1),
  SUM(target),
  SUM(target)::FLOAT / COUNT(1)::FLOAT AS conversion_rate
FROM
  analysis_observations_fit_for_training
WHERE 
  -- put node code here
 (COALESCE(is_personal,0) = 0 AND COALESCE(is_spam,0) = 0 AND COALESCE(is_student,0) = 0 AND COALESCE(is_edu,0) = 0 AND COALESCE(exclusions,0) = 0) AND (COALESCE(employees,-333) > 0) AND (COALESCE(employees,-333) < 100) AND (COALESCE(employees,-333) >= 10) AND (COALESCE(employees,-333) <= 50) AND (COALESCE(industry, 'unknown') IN ('Internet Software & Services', 'Professional Services')) AND  NOT (COALESCE(alexa_global, 0) > 4000000) AND  NOT (COALESCE(predicted_revenue_segment, 'unknown') = '1- less than $1m') AND (COALESCE(predicted_traffic, 0) > 25000)


