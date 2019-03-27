SET SEARCH_PATH TO mk_4006_1;


-- FOR TENANT 4006 --
-- get the account ids for accounts that became a paying customer in the L30 days
DROP TABLE IF EXISTS accounts_became_customer_last_30_days;
CREATE TEMP TABLE accounts_became_customer_last_30_days AS
SELECT
  a.source_key_value,
  a.a_stream_new__c,
  a.a_is_account_a_customer__c,
  a_sales_estimated_oppty_count__c,
  a.a_conversion_date__c,
  (c.email) AS email,
  MAX(c.a_mk_customer_fit_score__c) AS mk_customer_fit_score
--   MAX(c.a_mk_customer_fit_segment__c) AS mk_customer_fit_segment,
--   MAX(c.a_mk_customer_fit_signals__c) AS mk_customer_fit_signals
FROM
  crm_accounts AS a
LEFT JOIN
  crm_contacts c
  ON COALESCE(c.a_accountid, c.a_convertedaccountid) = a.source_key_value
  AND c.email IS NOT NULL
WHERE
  a.source_system = 'salesforce'
  AND a.source_system_object = 'Account'
  AND a.a_is_account_a_customer__c = 'true'
  AND a.a_stream_new__c IN ('Enterprise Direct', 'Enterprise Referral')
  -- might want to add OR condition here to include opps with more than 250 seats
  AND a.a_sales_estimated_oppty_count__c::FLOAT >= 250
  AND a.a_conversion_date__c >= GETDATE() - 90
  AND c.email NOT LIKE '%deputy.com'
GROUP BY
  1,2,3,4,5,6
;


-- get the list of domains with score <= 50 by MadKudu but has become a customer
SELECT DISTINCT
  mksco.email,
  source_key_value AS account_id,
-- --   mksco.mk_customer_fit_segment, -- they don't use segment
  CASE
    WHEN COALESCE(mk_customer_fit_score::FLOAT, 0) <= 15 THEN 'Tier 5'
    WHEN COALESCE(mk_customer_fit_score::FLOAT, 0) <= 24 THEN 'Tier 4'
    WHEN COALESCE(mk_customer_fit_score::FLOAT, 0) <= 50 THEN 'Tier 3'
  END AS tier,
  mksco.mk_customer_fit_score,
  a_is_account_a_customer__c,
  a_stream_new__c,
  a_sales_estimated_oppty_count__c,
  a_conversion_date__c,
  comp.employee_range,
  comp.industry,
  comp.company__country,
  comp.predicted_revenue_segment
--
-- --   mksco.mk_customer_fit_signals
FROM
  accounts_became_customer_last_30_days AS mksco
LEFT JOIN
  contacts_computations AS comp
  ON comp.email = mksco.email
WHERE
  mksco.mk_customer_fit_score::FLOAT <= 50
ORDER BY
  source_key_value DESC
;

WITH max_score AS (
    SELECT DISTINCT
      mksco.source_key_value AS account_id,
      --   MAX(mk_customer_fit_score) AS score,
      CASE
      WHEN COALESCE(MAX(mk_customer_fit_score) :: FLOAT, 0) <= 15
        THEN 'Tier 5'
      WHEN COALESCE(MAX(mk_customer_fit_score) :: FLOAT, 0) <= 24
        THEN 'Tier 4'
      WHEN COALESCE(MAX(mk_customer_fit_score) :: FLOAT, 0) <= 50
        THEN 'Tier 3'
      WHEN COALESCE(MAX(mk_customer_fit_score) :: FLOAT, 0) <= 74
        THEN 'Tier 2'
      ELSE 'Tier 1'
      END                    AS tier
    --   source_key_value AS account_id,
    --   mksco.mk_customer_fit_segment, -- they don't use segment

    FROM
      accounts_became_customer_last_30_days AS mksco
      LEFT JOIN
      contacts_computations AS comp
        ON comp.email = mksco.email
    -- WHERE
    --   mksco.mk_customer_fit_score::FLOAT <= 50
    -- ORDER BY
    --   source_key_value DESC
    GROUP BY 1
)
SELECT
  tier,
  COUNT(DISTINCT account_id)
FROM
  max_score
GROUP BY 1
;






-- check account counts by tier
--
-- SELECT
--   sf.Account_Tier__c,
--   COUNT(DISTINCT a.id)
-- FROM
--   crm_accounts a
-- LEFT JOIN
--   prototype_salesforce_raw_bulk sf
--   ON sf.id = a.source_key_value
-- WHERE
--   a.source_system = 'salesforce'
--   AND a.source_system_object = 'Account'
--   AND a.a_is_account_a_customer__c = 'true'
--   AND a.a_stream_new__c IN ('Enterprise Direct', 'Enterprise Referral')
--   AND a.a_conversion_date__c >= GETDATE() - 30
-- GROUP BY 1
-- ORDER BY 1 DESC
-- ;
--
-- SELECT COUNT(Id) FROM Account WHERE is_account_a_customer__c = true AND stream_new__c IN ('Enterprise Direct', 'Enterprise Referral') AND conversion_date__c = LAST_N_DAYS:90
--
-- SELECT
--   *
-- FROM
--   prototype_salesforce_raw_bulk






-- FALSE POSITIVES

-- try not a qualified opp after 30 days.

-- FOR TENANT 4006 --
-- get the contact ids and opportunity ids for opportunities won over the last 30 days
DROP TABLE IF EXISTS not_qualified_accounts_created_last_30_days;
CREATE TEMP TABLE not_qualified_accounts_created_last_30_days AS
SELECT
  DISTINCT a.id,
  a.source_key_value,
  a.a_stream_new__c,
  a_sales_estimated_oppty_count__c,
  c.email AS email,
  (c.a_mk_customer_fit_score__c) AS mk_customer_fit_score
FROM
  crm_accounts AS a
LEFT JOIN
  crm_opportunities o
  ON o.a_accountid = a.source_key_value
LEFT JOIN
  crm_contacts c
  ON COALESCE(c.a_accountid, c.a_convertedaccountid) = a.source_key_value
  AND c.email IS NOT NULL
WHERE
  a.source_system = 'salesforce'
  AND a.source_system_object = 'Account'
  AND COALESCE(a.a_is_account_a_customer__c, 'false') = 'false'
--   AND o.id IS NULL
  AND
  (
    COALESCE(a.a_qualified_oppty_indicator__c::FLOAT, 0) = 0
    OR
    COALESCE(a.a_number_of_opportunities__c::FLOAT, 0) = 0
  )
  AND a.a_createddate >= GETDATE() - 60
  AND a.a_createddate < GETDATE() - 30
-- GROUP BY
--   1,2,3,4
;

-- get the list of people scored highly that don't have a qualified opportunity
SELECT
  COUNT(DISTINCT comp.domain)
--   DISTINCT comp.domain,
--   mksco.source_key_value AS account_id,
--   MAX(mksco.mk_customer_fit_score),
--   MAX(comp.company__name) AS company_name,
--   MAX(comp.employees) AS employees,
--   MAX(comp.company__country) AS country,
--   MAX(comp.industry) AS industry,
--   MAX(comp.predicted_revenue_segment) AS predicted_revenue,
--   MAX(comp.company__type) AS company_type
FROM
  not_qualified_accounts_created_last_30_days AS mksco
INNER JOIN
  contacts_computations AS comp
  ON comp.email = mksco.email
WHERE
  mksco.mk_customer_fit_score::FLOAT > 50
-- GROUP BY 1,2
-- ORDER BY random()
LIMIT 100
;



