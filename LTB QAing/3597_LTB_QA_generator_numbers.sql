-- RUN LTB CODE 


-- AUDIENCE FOR GENERATOR
WITH cte AS (
	SELECT
		c.id AS lead_id,
		MAX(COALESCE(ns.logit, crm.logit, 0)) AS logit
	FROM
		crm_contacts c
	LEFT JOIN
		contacts AS co
		ON LOWER(co.email) = LOWER(c.email)
	LEFT JOIN
		analysis_scoring_conversion_free_trial_testing AS ns
		ON co.contact_id = ns.contact_id
  LEFT JOIN
		crm_analysis_scoring_conversion_testing AS crm
		ON c.source_system = crm.source_system
		AND c.source_system_object = crm.source_system_object
		AND c.source_key_name = crm.source_key_name
		AND c.source_key_value = crm.source_key_value
	WHERE
		-- Change based on your audience
		c.source_system = 'salesforce'
		AND c.source_system_object = 'Lead'
		AND LOWER(c.a_leadsource) NOT LIKE '%outbound%'
		AND c.created_date >= GETDATE() - 30
	GROUP BY
		1
)
SELECT
	logit,
	COUNT(1)
FROM
	cte
GROUP BY
	1
ORDER BY
	1 ASC
;

-- CONVERSION FOR GENERATOR
WITH cte AS (
	SELECT
		c.id AS opp_id,
		MAX(COALESCE(ns.logit, af.logit, 0)) AS logit
	FROM
		crm_contacts c
	LEFT JOIN
			contacts AS co
			ON LOWER(co.email) = LOWER(c.email)
	LEFT JOIN
			analysis_scoring_conversion_free_trial AS ns
			ON co.contact_id = ns.contact_id
	LEFT JOIN
		crm_analysis_scoring_conversion af
		ON c.source_system = af.source_system
		AND c.source_key_name = af.source_key_name
		AND c.source_system_object = af.source_system_object
		AND c.source_key_value = af.source_key_value
	LEFT JOIN
		crm_opportunities o
		ON o.a_accountid = COALESCE(c.a_convertedaccountid, c.a_accountid)
	WHERE
		c.source_system = 'salesforce'
		AND c.source_system_object = 'Lead'
		AND LOWER(c.a_leadsource) NOT LIKE '%outbound%'
		AND c.created_date >= GETDATE() - 30
			-- conversion = open opp
		AND o.a_type = 'New Customer'
	GROUP BY
		1
)
SELECT
	logit,
	COUNT(1)
FROM
	cte
GROUP BY
	1
ORDER BY
	1 ASC
;




-- AUDIENCE FOR GENERATOR
WITH cte AS (
	SELECT
		c.id AS lead_id,
		MAX(COALESCE(ns.logit, crm.logit, 0)) AS logit
	FROM
		crm_contacts c
	LEFT JOIN
		contacts AS co
		ON LOWER(co.email) = LOWER(c.email)
	LEFT JOIN
		analysis_scoring_conversion_free_trial AS ns
		ON co.contact_id = ns.contact_id
  LEFT JOIN
		crm_analysis_scoring_conversion AS crm
		ON c.source_system = crm.source_system
		AND c.source_system_object = crm.source_system_object
		AND c.source_key_name = crm.source_key_name
		AND c.source_key_value = crm.source_key_value
	WHERE
		-- Change based on your audience
		c.source_system = 'salesforce'
		AND c.source_system_object = 'Lead'
		AND LOWER(c.a_leadsource) NOT LIKE '%outbound%'
		AND c.created_date >= GETDATE() - 90
	GROUP BY
		1
)
SELECT
	logit,
	COUNT(1)
FROM
	cte
GROUP BY
	1
ORDER BY
	1 ASC
;



-- CONVERSION FOR GENERATOR
WITH cte AS (
	SELECT
		c.id AS opp_id,
		MAX(COALESCE(ns.logit, af.logit, 0)) AS logit
	FROM
		crm_contacts c
	LEFT JOIN
			contacts AS co
			ON LOWER(co.email) = LOWER(c.email)
	LEFT JOIN
			analysis_scoring_conversion_free_trial AS ns
			ON co.contact_id = ns.contact_id
	LEFT JOIN
		crm_analysis_scoring_conversion af
		ON c.source_system = af.source_system
		AND c.source_key_name = af.source_key_name
		AND c.source_system_object = af.source_system_object
		AND c.source_key_value = af.source_key_value
	WHERE
		c.source_system = 'salesforce'
		AND c.source_system_object = 'Lead'
		AND LOWER(c.a_leadsource) NOT LIKE '%outbound%'
		AND c.created_date >= GETDATE() - 90
			-- conversion = SQL
		AND LOWER(c.a_status) LIKE '%sales%qualified%'
	GROUP BY
		1
)
SELECT
	logit,
	COUNT(1)
FROM
	cte
GROUP BY
	1
ORDER BY
	1 ASC
;


