SET SEARCH_PATH TO mk_3733_1;

-- get all leads exported during downtime and their max scores
DROP TABLE IF EXISTS tmp_history;
CREATE TEMP TABLE tmp_history AS
SELECT
  source_key_value,
  MAX(mk_likelihood_to_buy_score) AS max_mk_likelihood_to_buy_score,
  MAX(mk_customer_fit_score) AS max_mk_customer_fit_score
FROM
  export_contacts_history
WHERE
  source_system = 'salesforce'
  AND source_system_object = 'Lead'
  AND created_at >= '2018-08-29'
  AND created_at <= '2018-09-04'
GROUP BY
  source_key_value
;


-- Get export from workbench:
-- run bulk query:

-- SELECT Id, Lead_from_Customer__c, Lead_from_Target_Account__c FROM Lead WHERE MQL_Start_Date__c = NULL AND OwnerId IN ('00Go0000002OK09', '00Go0000002WHAQ', '00Go0000002WKIE') AND Exclude_from_MQLs__c = false AND Event_in_Future__c = false AND Do_Not_Route__c = false AND (NOT Domain__c  like '%invision%') AND (NOT Domain__c  like '%havas%') AND DoNotCall = false AND Latest_Campaign_Type__c <> 'Other - Do Not Route' AND MK_is_Student__c = false

DROP TABLE IF EXISTS tmp_missed_mqls;
CREATE TEMP TABLE tmp_missed_mqls AS
SELECT
-- use this to count total
--   COUNT(DISTINCT h.source_key_value)

-- use this to see MQL reasons
--   (CASE
--           WHEN b.source_key_value IS NOT NULL AND h.max_mk_likelihood_to_buy_score >= 70 THEN 'rule 1'
--           WHEN (COALESCE(c.a_leadsource, 'unknwown') NOT IN ('Outbound')  AND h.max_mk_likelihood_to_buy_score >= 94 AND h.max_mk_customer_fit_score >= 80 AND lc.id IS NULL) THEN 'rule 2'
--           ELSE 'other'
--         END ) AS mql_reason

-- use these fields to create the table
  c.email,
  c.source_key_value,
  c.a_mql_start_date__c,
  h.max_mk_customer_fit_score,
  h.max_mk_likelihood_to_buy_score,
  b.Lead_from_Target_Account__c,
  b.Lead_from_Customer__c
FROM
  prototype_salesforce_raw_bulk b
INNER JOIN
  tmp_history h
  ON h.source_key_value = b.id
LEFT JOIN
  crm_contacts c
  ON c.source_key_value = h.source_key_value
  AND c.source_system_object = 'Lead'
WHERE
  (
    -- lead from target account and AND Behavioral Score is GREATER OR EQUAL 70
    (b.Lead_from_Target_Account__c = 'true' AND h.max_mk_likelihood_to_buy_score >= 70  )
    OR
    -- Lead Source is NOT EQUAL Outbound AND Behavioral Score is GREATER OR EQUAL 94 AND MK Demographic Score is GREATER OR EQUAL 80 AND Lead from Customer? EQUALS False
    (COALESCE(c.a_leadsource, 'unknwown') NOT IN ('Outbound')  AND h.max_mk_likelihood_to_buy_score >= 94 AND h.max_mk_customer_fit_score >= 80 AND b.Lead_from_Customer__c = 'false')
  )
--   AND COALESCE(c.a_isconverted, 'false') = 'false'
--   AND COALESCE(c.a_isdeleted, 'false') = 'false'

-- GROUP BY 2
-- ORDER BY 1 DESC
-- LIMIT 50
;


-- this checks the highest score that we successfully pushed
DROP TABLE IF EXISTS tmp_export_log_highest_scores;
CREATE TEMP TABLE tmp_export_log_highest_scores AS
SELECT
  m.source_key_value,
  MIN(m.Lead_from_Target_Account__c) AS Lead_from_Target_Account__c,
  MAX(JSON_EXTRACT_PATH_TEXT(payload, 'mk_customer_fit_score__c')) AS max_cf_score,
  MAX(JSON_EXTRACT_PATH_TEXT(payload, 'mk_likelihood_to_buy_score__c')) AS max_ltb_score
FROM
  tmp_missed_mqls m
LEFT JOIN
  export_contacts_log l
  ON m.source_key_value = l.source_key_value
  AND source_system = 'salesforce'
  AND l.source_system_object = 'Lead'
  AND success = TRUE
  AND push_timestamp >= GETDATE() - 90
GROUP BY
  1
;

-- this removes leads who we successfully pushed "qualifying scores" to get only true remaining new MQLs
DROP TABLE IF EXISTS leads_that_should_be_mqls;
CREATE TABLE leads_that_should_be_mqls AS
SELECT
  source_key_value
FROM
  tmp_export_log_highest_scores
WHERE
  -- checking never before pushed an MQL score
  (Lead_from_Target_Account__c = 'true' AND max_ltb_score < 70)
  OR
  (Lead_from_Target_Account__c = 'false' AND max_ltb_score < 94)
;


-- ROUND 2
-- get all leads exported during downtime and their max scores
DROP TABLE IF EXISTS tmp_history;
CREATE TABLE tmp_history AS
SELECT
  source_key_value,
  MAX(mk_likelihood_to_buy_score) AS max_mk_likelihood_to_buy_score,
  MAX(mk_customer_fit_score) AS max_mk_customer_fit_score
FROM
  export_contacts_history
WHERE
  source_system = 'salesforce'
  AND source_system_object = 'Lead'
  AND created_at >= '2018-08-29'
  AND created_at <= '2018-09-04'
GROUP BY
  source_key_value
;
-- DROP TABLE IF EXISTS list_target_accounts;
-- CREATE TABLE list_target_accounts AS

-- DROP TABLE IF EXISTS list_lead_from_customer;
-- CREATE TABLE list_lead_from_customer AS
-- SELECT
--   id AS source_key_value,
--   Lead_from_Customer__c
-- FROM
--   prototype_salesforce_raw_bulk b
-- ;


DROP TABLE IF EXISTS tmp_missed_mqls;
CREATE TEMP TABLE tmp_missed_mqls AS
SELECT
--   COUNT(DISTINCT h.source_key_value)

-- use this to see MQL reasons
--   (CASE
--           WHEN b.source_key_value IS NOT NULL AND h.max_mk_likelihood_to_buy_score >= 70 THEN 'rule 1'
--           WHEN (COALESCE(c.a_leadsource, 'unknwown') NOT IN ('Outbound')  AND h.max_mk_likelihood_to_buy_score >= 94 AND h.max_mk_customer_fit_score >= 80 AND lc.id IS NULL) THEN 'rule 2'
--           ELSE 'other'
--         END ) AS mql_reason


-- use these fields to create the table
  c.email,
  c.source_key_value,
  c.a_mql_start_date__c,
  h.max_mk_customer_fit_score,
  h.max_mk_likelihood_to_buy_score,
  b.Lead_from_Target_Account__c,
  b.Lead_from_Customer__c
FROM
  prototype_salesforce_raw_bulk b
INNER JOIN
  tmp_history h
  ON h.source_key_value = b.id
LEFT JOIN
  crm_contacts c
  ON c.source_key_value = h.source_key_value
  AND c.source_system_object = 'Lead'
WHERE
  (
    -- lead from target account and AND Behavioral Score is GREATER OR EQUAL 70
    (b.Lead_from_Target_Account__c = 'true' AND h.max_mk_likelihood_to_buy_score >= 70  )
    OR
    -- Lead Source is NOT EQUAL Outbound AND Behavioral Score is GREATER OR EQUAL 94 AND MK Demographic Score is GREATER OR EQUAL 80 AND Lead from Customer? EQUALS False
    (COALESCE(c.a_leadsource, 'unknwown') NOT IN ('Outbound')  AND h.max_mk_likelihood_to_buy_score >= 94 AND h.max_mk_customer_fit_score >= 80 AND b.Lead_from_Customer__c = 'false')
  )
--   AND COALESCE(c.a_isconverted, 'false') = 'false'
--   AND COALESCE(c.a_isdeleted, 'false') = 'false'

-- GROUP BY 2
-- ORDER BY 1 DESC
-- LIMIT 50
;

DROP TABLE IF EXISTS tmp_export_log_highest_scores;
CREATE TEMP TABLE tmp_export_log_highest_scores AS
SELECT
  m.source_key_value,
  MIN(m.Lead_from_Target_Account__c) AS Lead_from_Target_Account__c,
  MAX(JSON_EXTRACT_PATH_TEXT(payload, 'mk_customer_fit_score__c')) AS max_cf_score,
  MAX(JSON_EXTRACT_PATH_TEXT(payload, 'mk_likelihood_to_buy_score__c')) AS max_ltb_score
FROM
  tmp_missed_mqls m
LEFT JOIN
  export_contacts_log l
  ON m.source_key_value = l.source_key_value
  AND source_system = 'salesforce'
  AND l.source_system_object = 'Lead'
  AND success = TRUE
  AND push_timestamp >= GETDATE() - 90
GROUP BY
  1
;

DROP TABLE IF EXISTS leads_that_should_be_mqls;
CREATE TABLE leads_that_should_be_mqls AS
SELECT
  source_key_value
FROM
  tmp_export_log_highest_scores
WHERE
  -- checking never before pushed an MQL score
  (Lead_from_Target_Account__c = 'true' AND max_ltb_score < 70)
  OR
  (Lead_from_Target_Account__c = 'false' AND max_ltb_score < 94)
;