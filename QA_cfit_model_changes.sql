
-- Run cfit SQL code to generate temp temp_analysis_scoring_customer_fit table
DROP TABLE IF EXISTS tmp_audience;
CREATE TEMP TABLE tmp_audience AS
SELECT
  COALESCE(c.a_convertedaccountid, c.a_accountid, c.source_key_value) AS Id,
  MIN(CASE COALESCE(cf.customer_fit)
    WHEN 'very good' THEN '1 '
    WHEN 'good' THEN '2 '
    WHEN 'medium' THEN '3 '
    WHEN 'low' THEN '4 '
    ELSE '5 '
  END || COALESCE(cf.customer_fit, 'unknown')) AS segment,
  MIN(cf.email) AS email,
  MAX(cf.micro_ml) AS micro_ml
FROM
  crm_contacts c
LEFT JOIN
  crm_contacts co
  ON c.source_system = co.source_system
  AND co.source_system_object = 'Contact'
  AND c.a_convertedcontactid = co.source_key_value
LEFT JOIN
  temp_analysis_scoring_customer_fit cf
  ON LOWER(cf.email) = LOWER(c.email)
WHERE
  c.source_system = 'salesforce'
  AND c.source_system_object = 'Lead'
  AND COALESCE(c.a_isdeleted, 'false') = 'false'
  AND COALESCE(c.a_leadsource, 'Website') IN ('Website','Webinar','Partner','Google Adwords','Call-In','SEM','Referral','Social Media','Affiliate Referral', 'Trade Show', 'Conference / Tradeshow', 'Tradeshow')
GROUP BY
  1
HAVING
    MIN(c.a_createddate::TIMESTAMP) BETWEEN (GETDATE() - 180) AND (GETDATE())
;


DROP TABLE IF EXISTS tmp_conversion;
CREATE TEMP TABLE tmp_conversion AS
SELECT DISTINCT
  o.a_accountid AS Id,
  MIN(o.a_createddate::TIMESTAMP) AS a_createddate
FROM
  crm_opportunities o
WHERE
  o.source_system = 'salesforce'
  AND o.a_type = 'New Business'
  AND o.a_probability::FLOAT > 0
  AND o.a_amount::FLOAT > 240
GROUP BY
  1
;


-- Play with what to select and where conditions to sort by observation fields

SELECT
  a.email,
  c.a_mk_customer_fit_segment__c AS old_cfit,
  a.segment AS new_customer_fit,
  a.micro_ml,
  a.id,
  t.id,
  COALESCE(o.person__employment__title, o.person__employment__role) AS role,
  o.company__category__industry AS industry,
  o.company__metrics__employees AS company_size,
  o.company__geo__country,
  o.company__metrics__alexa_rank_global AS Alexa_rank,
  o.company__type,
  o.company__tags
FROM
  tmp_audience a
LEFT JOIN
  tmp_conversion t
  ON a.Id = t.Id
LEFT JOIN
  crm_contacts c
  ON LOWER(c.email) = LOWER(a.email)
  AND c.email IS NOT NULL
LEFT JOIN
  observations_firmographics o
  ON c.source_system = o.source_system
  AND c.source_system_object = o.source_system_object
  AND c.source_key_name = o.source_key_name
  AND c.source_key_value = o.source_key_value
WHERE
  c.a_mk_customer_fit_segment__c IN ('low', 'medium')
  AND COALESCE(o.person__employment__title, o.person__employment__role) IS NOT NULL
LIMIT 300
;