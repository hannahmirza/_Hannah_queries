SET SEARCH_PATH TO mk_3359_1;


-- Getting all export rows created today
-- ranking in order of "created_at"
DROP TABLE IF EXISTS temp_exports;
CREATE TEMP TABLE temp_exports AS
SELECT
	h.*,
	ROW_NUMBER() OVER (PARTITION BY h.email ORDER BY h.created_at::TIMESTAMP DESC NULLS LAST) AS rk
FROM
	export_contacts_history h
WHERE
	created_at >= GETDATE() - 2
ORDER BY
	email, rk ASC
;




-- create columns to flag which field caused the change
DROP TABLE IF EXISTS temp_change_reason;
CREATE TEMP TABLE temp_change_reason AS
SELECT
	r1.email,
	CASE WHEN r1.mk_likelihood_to_buy_segment <> r2.mk_likelihood_to_buy_segment THEN 1 ELSE 0 END AS mk_likelihood_to_buy_segment,
	CASE WHEN r1.mk_likelihood_to_buy_score <> r2.mk_likelihood_to_buy_score THEN 1 ELSE 0 END AS mk_likelihood_to_buy_score,
	CASE WHEN r1.mk_first_name <> r2.mk_first_name THEN 1 ELSE 0 END AS mk_first_name,
	CASE WHEN r1.mk_last_name <> r2.mk_last_name THEN 1 ELSE 0 END AS mk_last_name,
	CASE WHEN r1.mk_job_title <> r2.mk_job_title THEN 1 ELSE 0 END AS mk_job_title,
	CASE WHEN r1.mk_country <> r2.mk_country THEN 1 ELSE 0 END AS mk_country,
	CASE WHEN r1.mk_company_employees <> r2.mk_company_employees THEN 1 ELSE 0 END AS mk_company_employees,
	CASE WHEN r1.mk_company_industry <> r2.mk_company_industry THEN 1 ELSE 0 END AS mk_company_industry,
	CASE WHEN r1.mk_company_type <> r2.mk_company_type THEN 1 ELSE 0 END AS mk_company_type,
	CASE WHEN r1.mk_company_domain <> r2.mk_company_domain THEN 1 ELSE 0 END AS mk_company_domain,
	CASE WHEN r1.mk_company_alexa_us <> r2.mk_company_alexa_us THEN 1 ELSE 0 END AS mk_company_alexa_us,
	CASE WHEN r1.mk_has_done_edit_receipt_template_30d__c <> r2.mk_has_done_edit_receipt_template_30d__c THEN 1 ELSE 0 END AS mk_has_done_edit_receipt_template_30d__c,
	CASE WHEN r1.mk_count_of_sell_sale_created_30d__c <> r2.mk_count_of_sell_sale_created_30d__c THEN 1 ELSE 0 END AS mk_count_of_sell_sale_created_30d__c,
	CASE WHEN r1.mk_count_of_add_product_30d__c <> r2.mk_count_of_add_product_30d__c THEN 1 ELSE 0 END AS mk_count_of_add_product_30d__c,
	CASE WHEN r1.mk_predicted_plan <> r2.mk_predicted_plan THEN 1 ELSE 0 END AS mk_predicted_plan,
	CASE WHEN r1.mk_lead_potential <> r2.mk_lead_potential THEN 1 ELSE 0 END AS mk_lead_potential

FROM
	temp_exports r1
INNER JOIN
	temp_exports r2
	ON r2.email = r1.email
	AND r2.rk = 2
WHERE
	r1.rk = 1
;



-- Summing each column to find the biggest offenders

SELECT
	COUNT(DISTINCT email),
	SUM(mk_likelihood_to_buy_score) AS mk_likelihood_to_buy_score_changed,
	SUM(mk_first_name) AS mk_first_name_changed,
	SUM(mk_last_name) AS mk_last_name_changed,
	SUM(mk_job_title) AS mk_job_title_changed,
	SUM(mk_country) AS mk_country_changed,
	SUM(mk_company_employees) AS mk_company_employees_changed,
	SUM(mk_company_industry) AS mk_company_industry_changed,
	SUM(mk_company_type) AS mk_company_type_changed,
	SUM(mk_company_domain) AS mk_company_domain_changed,
	SUM(mk_company_alexa_us) AS mk_company_alexa_us_changed,
	SUM(mk_has_done_edit_receipt_template_30d__c) AS mk_has_done_edit_receipt_template_30d__c_changed,
	SUM(mk_count_of_sell_sale_created_30d__c) AS mk_count_of_sell_sale_created_30d__c_changed,
	SUM(mk_count_of_add_product_30d__c) AS mk_count_of_add_product_30d__c_changed,
	SUM(mk_predicted_plan) AS mk_predicted_plan_changed,
	SUM(mk_lead_potential) AS mk_lead_potential_changed
FROM
  temp_change_reason;