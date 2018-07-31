-- VEND False Neg QA
SET SEARCH_PATH TO mk_3359_1;

-- ======================== CFit Model ========================

DROP TABLE IF EXISTS tmp_all_contacts;
CREATE TEMP TABLE tmp_all_contacts AS
SELECT
      c.source_system,
      c.source_system_object,
      c.source_key_name,
      c.source_key_value,
      LOWER(c.email) AS email,
      LOWER(c.domain) AS domain,
      COALESCE(crm.a_company, c.a_company) AS a_company,
      -- those are self-identified customer data
      -- Removed on 2018-03-01 by Hannah as requested by Cath
      -- No longer use this field
      -- crm.a_trial_employee_count__c,
      crm.a_trial_outlet_type__c,
      crm.a_trial_pricing_plan__c,
      crm.a_vertical_industry__c,
      -- for exclusions
      crm.a_partner__c,
      crm.a_advisor__c,
      crm.a_enquiry_topic__c,
      crm.a_phone,
      crm.a_country,
      -- Salesforce has multiple records for the same email, so we want to pick one with values
      ROW_NUMBER() OVER (
        PARTITION BY
          c.source_system,
          c.source_system_object,
          c.source_key_name,
          c.source_key_value
        ORDER BY
          -- Removed on 2018-03-01 by Hannah as requested by Cath
          -- No longer use this field
          --crm.a_trial_employee_count__c DESC NULLS LAST,
          crm.a_trial_outlet_type__c DESC NULLS LAST,
          crm.a_trial_pricing_plan__c DESC NULLS LAST,
          crm.a_vertical_industry__c DESC NULLS LAST
      ) AS rk
FROM
      contacts AS c
LEFT JOIN
      crm_contacts AS crm
      ON LOWER(c.email) = LOWER(crm.email)
      AND crm.source_system = 'salesforce'
;

-- Remove dupes
DELETE FROM tmp_all_contacts WHERE rk > 1;

DROP TABLE IF EXISTS tmp_company_email;
CREATE TEMP TABLE tmp_company_email AS
SELECT DISTINCT
      TRIM(LOWER(c.email)) AS email
FROM
      crm_contacts c
INNER JOIN
      companies co
      ON LOWER(c.domain) = LOWER(co.mk_domain)
      AND co.is_personal = true
INNER JOIN
      persons p
      ON LOWER(c.email) = LOWER(p.mk_email)
      AND COALESCE(p.is_spam, false) = false
WHERE
      LENGTH(c.a_company) > 5
      AND STRPOS(TRIM(LOWER(SPLIT_PART(c.email, '@', 1))), REPLACE(TRIM(LOWER(c.a_company)), ' ', '')) > 0
      AND LOWER(a_firstname) <> REPLACE(TRIM(LOWER(c.a_company)), ' ', '')
      AND LOWER(a_lastname) <> REPLACE(TRIM(LOWER(c.a_company)), ' ', '')
;

DROP TABLE IF EXISTS tmp_analysis_observations_fit;
CREATE TEMP TABLE tmp_analysis_observations_fit AS
SELECT DISTINCT
      c.source_system,
      c.source_system_object,
      c.source_key_name,
      c.source_key_value,
      c.email,

      CASE
        WHEN o.company__is_edu = true AND o.person__is_student = false THEN -10
        WHEN o.person__is_student = true THEN -200
        WHEN c.email like '%.edu' THEN -200
        WHEN c.email like '%.edu.%' THEN -200
        WHEN LOWER(COALESCE(o.person__employment__title, o.person__employment__role)) LIKE '%student%' THEN -200
        ELSE 0
      END AS mk_edu_score,

      CASE
        WHEN o.person__is_spam = true THEN -200
        -- test email, please ignore
        WHEN lower(c.email) LIKE '%test%' THEN -200
        -- one letter either before or after @
        WHEN CHARINDEX('@',c.email) = 2 THEN -200
        WHEN CHARINDEX('.',c.domain) = 2 THEN -200
        -- bad words
        WHEN LOWER(c.email) LIKE '%fuck%' THEN -50
        WHEN LOWER(c.email) LIKE '%cunt%' THEN -50
        WHEN LOWER(c.email) LIKE '%dick%' THEN -50
        -- email contains numbers
        WHEN REGEXP_COUNT(SPLIT_PART(c.email, '@',1), '[0-9]') > 0 THEN -100
        -- contains a QWERTY sequence multiple times
        WHEN REGEXP_COUNT(LOWER(c.email), 'asd') > 1 THEN -100
        WHEN REGEXP_COUNT(LOWER(c.email), 'sdf') > 1 THEN -100
        WHEN REGEXP_COUNT(LOWER(c.email), 'asdf') > 0 THEN -100
        -- no vowels
        WHEN LEN(SPLIT_PART(c.email, '@',1)) > 3 AND REGEXP_COUNT(LOWER(SPLIT_PART(c.email, '@',1)), '(a|e|i|o|u|y)') = 0 THEN -50
        ELSE 0
      END AS mk_spam_score,

      CASE
        WHEN o.company__metrics__employees >= 500 THEN 5
        WHEN o.company__metrics__employees >= 100 THEN 7
        WHEN o.company__metrics__employees >= 10 THEN 10
        ELSE -2
      END AS mk_employee_score,

      CASE
        WHEN LOWER(o.person__employment__title) LIKE '%retail%' THEN 5
        WHEN LOWER(o.person__employment__role) LIKE '%owner%' THEN 5
        WHEN LOWER(o.person__employment__role) LIKE '%finance%' THEN 4
        WHEN LOWER(o.person__employment__role) LIKE '%engineering%' THEN 4
        WHEN LOWER(o.person__employment__role) LIKE '%operations%' THEN 3
        WHEN COALESCE(o.person__employment__title, o.person__employment__role) IS NOT NULL THEN 1
        ELSE 0
      END AS mk_role_score,

      CASE
        WHEN LOWER(o.person__employment__seniority) = 'founder' THEN 5
        WHEN LOWER(o.person__employment__seniority) = 'executive' THEN 4
        WHEN LOWER(o.person__employment__seniority) = 'director' THEN 3
        WHEN LOWER(o.person__employment__seniority) = 'manager' THEN 2
        WHEN o.person__employment__seniority IS NOT NULL THEN 1
        ELSE 0
      END AS mk_seniority_score,

      CASE
        -- Great
        WHEN o.company__category__industry_group = 'Consumer Durables & Apparel' THEN 10
        WHEN o.company__category__industry = 'Specialty Retail' THEN 10
        WHEN o.company__category__industry = 'Food Products' THEN 5 -- In Consumer Staples Sector
        WHEN o.company__category__sector = 'Consumer Staples' THEN 10
        WHEN o.company__category__industry = 'Health Care Providers & Services' THEN 10

        -- Good
        WHEN o.company__category__industry = 'Automotive' THEN 7
        WHEN o.company__category__industry = 'Consumer Discretionary' THEN 7
        WHEN o.company__category__industry = 'Distributors' THEN -5 -- In Retailing Industry Group
        WHEN o.company__category__industry_group = 'Retailing' THEN 7

        -- Ok
        WHEN o.company__category__industry_group = 'Health Care Equipment & Services' THEN 5
        WHEN o.company__category__industry_group = 'Pharmaceuticals' THEN 5
        WHEN o.company__category__industry = 'Commercial Services & Supplies' THEN 5
        WHEN o.company__category__industry = 'Building Materials' THEN 5
        WHEN o.company__category__industry = 'Construction Materials' THEN 5
        WHEN o.company__category__industry = 'Wireless Telecommunication Services' THEN 5

        -- Everything else is bad
        WHEN COALESCE(o.company__category__industry, o.company__category__industry_group, o.company__category__sector) IS NOT NULL THEN -20
        ELSE 0
      END AS mk_industry_score,

      CASE
        -- Great
        WHEN LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) LIKE '%australia%' THEN 5
        WHEN LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) LIKE '%new zealand%' THEN 5
        WHEN LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) LIKE '%hong kong%' THEN 5
        WHEN LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) LIKE '%singapore%' THEN 5

        -- Good
        WHEN LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) LIKE '%united states%' THEN 4
        WHEN LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) LIKE '%united kingdom%' THEN 4
        WHEN LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) LIKE '%canada%' THEN 4

        -- Ok
        WHEN LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) LIKE '%south africa%' THEN 3
        WHEN LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) LIKE '%france%' THEN 2
        WHEN LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) LIKE '%germany%' THEN 2
        WHEN LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) LIKE '%mexico%' THEN 2

        -- Bad
        WHEN LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) LIKE '%india%' THEN -15
        WHEN COALESCE(o.company__geo__country, o.person__geo__country, c.a_country) IS NOT NULL THEN -5
        ELSE 0
      END AS mk_country_score,

      CASE LOWER(o.company__type)
        WHEN 'private' THEN 5
        WHEN 'personal' THEN 4
        WHEN 'education' THEN 1
        WHEN 'nonprofit' THEN 0
        WHEN 'public' THEN -5
        WHEN 'government' THEN -5
        ELSE 0
      END AS mk_companytype_score,

      CASE
        WHEN UPPER(o.company__tags) LIKE '%B2C%' AND UPPER(o.company__tags) LIKE '%B2B%' THEN 3
        WHEN UPPER(o.company__tags) LIKE '%B2B%' THEN -2
        WHEN UPPER(o.company__tags) LIKE '%B2C%' THEN 10
        ELSE 0
      END AS mk_b2x_score,

      CASE
        WHEN LOWER(o.company__tags) LIKE '%retail%' THEN 10
        WHEN LOWER(o.company__tags) LIKE '%supermarket%' THEN -5
        WHEN LOWER(o.company__tags) LIKE '%e-commerce%' THEN 5
        ELSE 0
      END AS mk_retail_score,

      -- Removed on 2018-03-01 by Hannah as requested by Cath
      -- No longer use this field
      -- CASE
      --   WHEN c.a_trial_employee_count__c = '4+' THEN 3
      --   ELSE 0
      -- END AS mk_trial_employee_count,

      CASE
        WHEN c.a_trial_outlet_type__c IN ('2-5', '6-14', '15+', 'Multi') THEN 3
        WHEN c.a_trial_outlet_type__c IN ('1', 'Single') THEN 1
        ELSE 0
      END AS mk_trial_outlet_type,

      CASE
        WHEN c.a_trial_pricing_plan__c = 'Free' THEN -1
        ELSE 0
      END AS mk_trial_pricing_plan,

      CASE
        WHEN c.a_vertical_industry__c IN ('Fashion/Apparel Retailing', 'Sport and Camping Equipment Retailing', 'Newspaper, Books, Stationary Retailing', 'Furniture Retailing', 'Watch and Jewellery Retailing', 'Domestic Hardware and Houseware Retailing', 'Footwear Retailing', 'Domestic Appliance Retailing', 'Clothing Stores', 'Fashion & Apparel', 'Sport & Outdoors', 'Gift Shops', 'Home, Lifestyle & Gifts', 'Sports, Hobbies & Toys') THEN 3 --Great
        WHEN c.a_vertical_industry__c IN ('Specialised Food Retailing', 'Pharmaceutical, Cosmetic, Toiletries', 'Liquor Retailing', 'Bread and Cake Retailing', 'Electronics & Computer', 'Recorded Music Retailing', 'Automotive Retailing', 'Car/Motor Cycle Retailing') THEN 2 --Good
        WHEN c.a_vertical_industry__c IN ('Supermarket and Grocery Stores', 'Department Stores', 'Toy and Game Retailing', 'Antique and Used Goods Retailing', 'Garden Supplies Retailing', 'Flower Retailing', 'Floor Covering Retailing', 'Photographic Equipment Retailing', 'Fabrics and other Soft Goods Retail', 'Marine Equipment Retailing', 'Other retail', 'Other') THEN 1 --Ok
        WHEN c.a_vertical_industry__c IN ('Hospitality/Cafe', 'Takeaway Food Retailing', 'Food & Drink retail', 'Household Equipment Repair Service') THEN -3 --Bad
        ELSE 0
      END AS mk_market_segment,

      -- These three sections allow us to override certain RBML scoring we do
      -- Minimum segment ensures that people who qualifiy a set of requirements are "at least" this good
      -- Maximum segment caps the highest segment that this person can have
      -- Exclude is a hard exclusion to low
      CASE
        WHEN
          c.a_trial_outlet_type__c IN ('2-5', '6-14', '15+', 'Multi')
          AND c.a_vertical_industry__c IN ('Fashion & Apparel', 'Home, Lifestyle & Gifts', 'Sports, Hobbies & Toys', 'Health & Beauty Retail')
          AND LOWER(COALESCE(o.company__geo__country, o.person__geo__country, c.a_country)) IN ('australia', 'new zealand', 'united kingdom', 'united states', 'canada', 'singapore', 'south africa')
          THEN 'medium'
      END AS minimum_segment,

      CASE
        WHEN COALESCE(LENGTH(COALESCE(o.company__phone, c.a_phone)), 0) <= 5 THEN 'good'
        ELSE 'very good' -- default maximum
      END AS maximum_segment,

      CASE
        -- Request from Cath to exclude these people
        WHEN c.a_enquiry_topic__c LIKE 'Becoming a Vend Expert%' THEN 1
        WHEN c.a_enquiry_topic__c LIKE 'Becoming a developer%' THEN 1
        WHEN c.a_partner__c = 'true' AND c.a_advisor__c = 'true' THEN 1
        WHEN c.a_partner__c = 'true' AND c.a_advisor__c = 'true' THEN 1
        WHEN LOWER(c.a_company) LIKE '%catering%' OR LOWER(o.company__name) LIKE '%catering%' OR c.email LIKE '%catering%' THEN 1 -- Request by Cath on 2017-07-31: hard exclude catering companies
        WHEN ce.email IS NOT NULL THEN 0 -- These are personal emails that represent a company (e.g. madkudu@gmail.com). We don't want to hard exclude these people
        WHEN o.company__emailprovider = true THEN 1
        WHEN o.company__is_personal = true THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'xero.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'deputy.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'unleashedsoftware.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'ccecentrix.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'maestrano.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'consilia.co.za' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'collectrewardsapp.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'paymentexpress.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'dearsystems.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'interac.ca' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'e-hps.co' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'linksync.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'smartpay.co.nz' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'unitedhospitalitygroup.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'ideahall.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'midco.net' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'collectapps.io' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'gettimely.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'squareup.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'mullenlowie.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'symplexity.nz' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'salesforce.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = '3circlesoft.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'ecwid.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'yahoo-inc.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'john.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'bnc.ca' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'alvarezandmarsal.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'ap.jll.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = '163.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'cognizant.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'slalom.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'curalate.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'sicamdev.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'bybubbly.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'stanjohnsonco.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'wheniwork.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'apple.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'thebox.com.au' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'sacredmysteries.com.au' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'auspost.com.au' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'aha.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'cimation.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'simpletechllc.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'ttlbusiness.co.nz' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'vantiv.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'dfdf.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'shopify.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'metro.ca' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'myob.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'huawei.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'sjog.org.au' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'nova.com.hk' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'auspost.com.au' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'elotouch.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'truepaymentsolutions.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'berndtcpa.com ' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'mylindo.com ' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'sm-int.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'myagi.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'star-emea.com' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'unl.sh' THEN 1
        WHEN COALESCE(o.domain, c.domain, 'NULL') = 'onesourcebt.com' THEN 1
        WHEN LOWER(c.email) LIKE '%+hackerone%' THEN 1
        WHEN LOWER(c.email) LIKE '%@raywhite.com' THEN 1
        ELSE 0
      END AS exclude

FROM
      tmp_all_contacts AS c
LEFT JOIN
      observations_firmographics AS o
      ON c.source_system = o.source_system
      AND c.source_system_object = o.source_system_object
      AND c.source_key_name = o.source_key_name
      AND c.source_key_value = o.source_key_value
LEFT JOIN
      tmp_company_email AS ce
      ON LOWER(c.email) = LOWER(ce.email)
;

DROP TABLE IF EXISTS temp_analysis_scoring_customer_fit_test;
CREATE TABLE temp_analysis_scoring_customer_fit_test AS
WITH cte_logit AS
(
  SELECT
        o.source_system,
        o.source_system_object,
        o.source_key_name,
        o.source_key_value,
        o.email,
        o.minimum_segment,
        o.maximum_segment,
        o.exclude,
        COALESCE((o.mk_edu_score + o.mk_spam_score + o.mk_employee_score + o.mk_role_score + o.mk_seniority_score + o.mk_industry_score + o.mk_country_score + o.mk_companytype_score + o.mk_b2x_score + o.mk_retail_score + o.mk_trial_outlet_type + o.mk_trial_pricing_plan + o.mk_market_segment), 0) AS logit,
        ROW_NUMBER() OVER ( PARTITION BY o.source_system,o.source_system_object,o.source_key_name,o.source_key_value
              ORDER BY (o.mk_edu_score + o.mk_spam_score + o.mk_employee_score + o.mk_role_score + o.mk_seniority_score + o.mk_industry_score + o.mk_country_score + o.mk_companytype_score + o.mk_b2x_score + o.mk_retail_score + o.mk_trial_outlet_type + o.mk_trial_pricing_plan + o.mk_market_segment) DESC NULLS LAST
      ) AS rk
  FROM
        tmp_analysis_observations_fit AS o
)
SELECT
      source_system,
      source_system_object,
      source_key_name,
      source_key_value,
      email,
      exclude,
      logit,
      -- Modified by Hac Phan on 2017-07-06
      -- The business rules determine the minimum segment that this contact should be in. Thus, we put it after all the rule that would put it in a higher segment
      -- We have a minimum segment and a maximum segment
      -- If someone were to ever have conflicting min and max (e.g. min is "very good" and max is "good"), the logic prioritizes the minimum segment rule
      CASE
        WHEN exclude = 1 THEN 'low'
        WHEN maximum_segment IN ('very good') AND logit >= 35 THEN 'very good'
        WHEN minimum_segment = 'very good' THEN 'very good'
        WHEN maximum_segment IN ('good', 'very good') AND logit >= 15 THEN 'good'
        WHEN minimum_segment = 'good' THEN 'good'
        WHEN maximum_segment IN ('medium', 'good', 'very good') AND logit >= 0 THEN 'medium'
        WHEN minimum_segment = 'medium' THEN 'medium'
        ELSE 'low'
      END AS customer_fit,
      'old_sql_rules' AS prediction_source
FROM
      cte_logit
WHERE
      rk = 1
;



--
-- -- ======================== No Exclusions ========================
DROP TABLE IF EXISTS temp_analysis_scoring_customer_fit_test_no_excl;
CREATE TABLE temp_analysis_scoring_customer_fit_test_no_excl AS
WITH cte_logit AS
(
  SELECT
        o.source_system,
        o.source_system_object,
        o.source_key_name,
        o.source_key_value,
        o.email,
        o.minimum_segment,
        o.maximum_segment,
        o.exclude,
        COALESCE((o.mk_edu_score + o.mk_spam_score + o.mk_employee_score + o.mk_role_score + o.mk_seniority_score + o.mk_industry_score + o.mk_country_score + o.mk_companytype_score + o.mk_b2x_score + o.mk_retail_score + o.mk_trial_outlet_type + o.mk_trial_pricing_plan + o.mk_market_segment), 0) AS logit,
        ROW_NUMBER() OVER ( PARTITION BY o.source_system,o.source_system_object,o.source_key_name,o.source_key_value
              ORDER BY (o.mk_edu_score + o.mk_spam_score + o.mk_employee_score + o.mk_role_score + o.mk_seniority_score + o.mk_industry_score + o.mk_country_score + o.mk_companytype_score + o.mk_b2x_score + o.mk_retail_score + o.mk_trial_outlet_type + o.mk_trial_pricing_plan + o.mk_market_segment) DESC NULLS LAST
      ) AS rk
  FROM
        tmp_analysis_observations_fit AS o
)
SELECT
      source_system,
      source_system_object,
      source_key_name,
      source_key_value,
      email,
      exclude,
      logit,
      -- Modified by Hac Phan on 2017-07-06
      -- The business rules determine the minimum segment that this contact should be in. Thus, we put it after all the rule that would put it in a higher segment
      -- We have a minimum segment and a maximum segment
      -- If someone were to ever have conflicting min and max (e.g. min is "very good" and max is "good"), the logic prioritizes the minimum segment rule
      CASE
--         WHEN exclude = 1 THEN 'low'
        WHEN maximum_segment IN ('very good') AND logit >= 35 THEN 'very good'
        WHEN minimum_segment = 'very good' THEN 'very good'
        WHEN maximum_segment IN ('good', 'very good') AND logit >= 15 THEN 'good'
        WHEN minimum_segment = 'good' THEN 'good'
        WHEN maximum_segment IN ('medium', 'good', 'very good') AND logit >= 0 THEN 'medium'
        WHEN minimum_segment = 'medium' THEN 'medium'
        ELSE 'low'
      END AS customer_fit,
      'old_sql_rules' AS prediction_source
FROM
      cte_logit
WHERE
      rk = 1
;

-- -- ======================== ^^^ No Exclusions ========================

DROP TABLE IF EXISTS tmp_exclusions;
-- CREATE TEMP TABLE tmp_exclusions AS
SELECT
  cf.customer_fit,
  count(DISTINCT jan.source_key_value)
FROM
  tmp_vend_jan jan
LEFT JOIN
  crm_contacts c
  ON c.source_key_value =jan.source_key_value
  AND source_system = 'salesforce'
  AND source_system_object = 'Lead'
LEFT JOIN
  analysis_scoring_customer_fit cf
  ON cf.email = c.email
WHERE
  cf.email IS NOT NULL
GROUP BY 1
;

-- excluded leads scored
SELECT
  cf.customer_fit,
  COUNT(DISTINCT e.source_key_value)
FROM
  tmp_exclusions e
LEFT JOIN
  crm_contacts c
  ON c.source_key_value = e.source_key_value
  AND source_system = 'salesforce'
  AND source_system_object = 'Lead'
LEFT JOIN
  temp_analysis_scoring_customer_fit_test_no_excl cf
  ON cf.email = c.email
GROUP BY
  1
;

