-- RUN LTB CODE

-- RUN LG CODE

-- COMPUTATION



SELECT
	CASE WHEN c.a_status = 'Marketing Qualified' THEN'MQL' ELSE 'Not MQL' END as old_mql_count,
-- 	CASE WHEN c.a_status = 'Marketing Qualified' OR (p.mk_lead_grade IN ('A', 'B', 'C') AND p.mk_likelihood_to_buy_segment NOT IN ('low', 'not applicable')) THEN 'MQL' ELSE 'Not MQL' END as new_mql_count,
	COUNT(DISTINCT c.id)
FROM
	crm_contacts c
LEFT JOIN
	prototype_lead_grade_testing p
	ON p.source_system = c.source_system
	AND p.source_system_object = c.source_system_object
	AND p.source_key_name = c.source_key_name
	AND p.source_key_value = c.source_key_value
WHERE
	a_createddate >=GETDATE() - 90
GROUP BY 1
;



WITH cte AS (
	SELECT
		c.id,
		CASE WHEN c.a_status = 'Marketing Qualified' THEN 'MQL' ELSE 'Not MQL' END AS mql,
		CASE WHEN p.mk_lead_grade IN ('A', 'B', 'C') AND p.mk_likelihood_to_buy_segment NOT IN ('low', 'not applicable') THEN 'MQL' ELSE 'Not MQL' END AS new_mql
	FROM
		crm_contacts c
	LEFT JOIN
		prototype_lead_grade_testing p
		ON p.source_system = c.source_system
		AND p.source_system_object = c.source_system_object
		AND p.source_key_name = c.source_key_name
		AND p.source_key_value = c.source_key_value
	WHERE
		c.source_system_object = 'Lead'
		AND c.a_leadsource NOT LIKE '%outbound%'
	-- don't add condition to see how many statuses will change in total
	-- add condition to see how model "would have performed x days ago"
		AND c.created_date > GETDATE() - 30
)
SELECT
	COUNT(DISTINCT id),
	-- computes changes in status
	COUNT(DISTINCT CASE WHEN mql = 'MQL' and new_mql = 'MQL' THEN id END) AS before_and_after_status_is_mql,
	COUNT(DISTINCT CASE WHEN mql <> 'MQL' and new_mql = 'MQL' THEN id END) AS net_new_mql,
	-- computes what new numbers will be
	COUNT(DISTINCT CASE WHEN new_mql <> 'MQL' THEN id END) AS new_count_not_mql,
	COUNT(DISTINCT CASE WHEN new_mql = 'MQL' THEN id END) AS new_count_mql
FROM
	cte
;


