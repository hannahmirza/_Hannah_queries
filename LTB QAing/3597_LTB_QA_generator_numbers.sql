SET SEARCH_PATH TO mk_3597_1;

-- ===============================
-- Prep
-- ===============================
DROP TABLE IF EXISTS tmp_aggregation_lifetime;
CREATE TEMP TABLE tmp_aggregation_lifetime AS
SELECT
			c2a.account_id,
			DATEDIFF(DAYS, MAX(e.event_timestamp), GETDATE()) AS mk_account_days_since_last_seen,
			DATEDIFF(DAYS, MIN(e.event_timestamp), GETDATE()) AS mk_account_days_since_first_seen,
			COUNT(DISTINCT (DATEPART(DAY, e.event_timestamp)::VARCHAR(2) + '-' + DATEPART(MONTH, e.event_timestamp)::VARCHAR(2))) AS mk_account_number_of_active_days
FROM
			events AS e
INNER JOIN
			contacts_to_accounts c2a
			ON e.contact_id = c2a.contact_id
GROUP BY
			c2a.account_id
;

DROP TABLE IF EXISTS tmp_aggregation_l30d;
CREATE TEMP TABLE tmp_aggregation_l30d AS
SELECT
			c2a.account_id,
			COUNT(e.event_timestamp) AS mk_account_number_of_events_last_30_days,
			COUNT(CASE WHEN e.meta_event IN ('viewed_careers_page') THEN e.event_timestamp ELSE NULL END) AS mk_account_number_of_viewed_careers_page_last_30_days,
			COUNT(CASE WHEN e.meta_event LIKE '%view%' THEN e.event_timestamp ELSE NULL END) AS mk_account_number_of_view_events_last_30_days,
			COUNT(CASE WHEN e.meta_event LIKE 'content%' THEN e.event_timestamp ELSE NULL END) AS mk_account_number_of_content_last_30_days,
			COUNT(CASE WHEN e.meta_event LIKE '%click' THEN e.event_timestamp ELSE NULL END) AS mk_account_number_of_click_last_30_days,
			COUNT(CASE WHEN e.meta_event LIKE '%email_delivered%' THEN e.event_timestamp ELSE NULL END) AS mk_account_number_of_email_delivered_last_30_days,
			COUNT(CASE WHEN e.meta_event LIKE '%email_opened%' THEN e.event_timestamp ELSE NULL END) AS mk_account_number_of_email_opened_last_30_days,
			COUNT(CASE WHEN e.meta_event LIKE '%email_link_clicked%' THEN e.event_timestamp ELSE NULL END) AS mk_account_number_of_email_link_clicked_last_30_days,
			COUNT(CASE WHEN e.meta_event LIKE '%subscribed%' THEN e.event_timestamp ELSE NULL END) AS mk_account_number_of_subscribed_last_30_days,
			COUNT(CASE WHEN e.meta_event LIKE '%email_bounced%' THEN e.event_timestamp ELSE NULL END) AS mk_account_number_of_email_bounced_last_30_days,
			COUNT(CASE WHEN e.meta_event LIKE '%unsubscribed%' THEN e.event_timestamp ELSE NULL END) AS mk_account_number_of_unsubscribed_last_30_days,
			COUNT(CASE WHEN e.meta_event LIKE '%demo' THEN e.event_timestamp ELSE NULL END) AS mk_account_number_of_demo_requests_last_30_days
FROM
			events AS e
INNER JOIN
			contacts_to_accounts c2a
			ON e.contact_id = c2a.contact_id
WHERE
			DATEDIFF(DAY, e.event_timestamp, GETDATE()) <= 30
GROUP BY
			c2a.account_id
;

-- Added 2018-02-28 by Hannah to include Salesforce Campaigns in L2B computation
DROP TABLE IF EXISTS tmp_aggregation_crm_event;
CREATE TEMP TABLE tmp_aggregation_crm_event AS
SELECT
      LOWER(c.email) AS email,
      MAX(CASE WHEN meta_event = 'field_event_attended' THEN 5 ELSE 0 END) AS field_event_attended,
      MAX(CASE WHEN meta_event = 'field_event_registered' THEN 2 ELSE 0 END) AS field_event_registered,
      MAX(CASE WHEN meta_event = 'sponsered_event_visited_booth_hot' THEN 10 ELSE 0 END) AS sponsered_event_visited_booth_hot,
      MAX(CASE WHEN meta_event = 'sponsered_event_visited_booth_warm' THEN 5 ELSE 0 END) AS sponsered_event_visited_booth_warm,
      MAX(CASE WHEN meta_event = 'sponsered_event_visited_booth_cold' THEN 1 ELSE 0 END) AS sponsered_event_visited_booth_cold,
      MAX(CASE WHEN meta_event = 'BOFU_content_campaign_visits_landing_page' THEN 1 ELSE 0 END) AS BOFU_content_campaign_visits_landing_page,
      MAX(CASE WHEN meta_event = 'BOFU_content_campaign_filled_out_form' THEN  3 ELSE 0 END) AS BOFU_content_campaign_filled_out_form,
      MAX(CASE WHEN meta_event = 'BOFU_content_campaign_engages_with_content' THEN 5 ELSE 0 END) AS BOFU_content_campaign_engages_with_content,
      MAX(CASE WHEN meta_event = 'TOFU_content_campaign_visits_landing_page' THEN 1 ELSE 0 END) AS TOFU_content_campaign_visits_landing_page,
      MAX(CASE WHEN meta_event = 'TOFU_content_campaign_filled_out_form' THEN 2 ELSE 0 END) AS TOFU_content_campaign_filled_out_form,
      MAX(CASE WHEN meta_event = 'TOFU_content_campaign_engages_with_content' THEN 3 ELSE 0 END) AS TOFU_content_campaign_engages_with_content,
      MAX(CASE WHEN meta_event = 'Sponsored_webinar_registered' THEN 2 ELSE 0 END) AS Sponsored_webinar_registered,
      MAX(CASE WHEN meta_event = 'Sponsored_webinar_attended' THEN 3 ELSE 0 END) AS Sponsored_webinar_attended,
      MAX(CASE WHEN meta_event = 'Sponsored_webinar_attended_on_demand' THEN 5 ELSE 0 END) AS Sponsored_webinar_attended_on_demand,
      MAX(CASE WHEN meta_event = 'Produced_webinar_registered' THEN 1 ELSE 0 END) AS Produced_webinar_registered,
      MAX(CASE WHEN meta_event = 'Produced_webinar_attended' THEN 1 ELSE 0 END) AS Produced_webinar_attended,
      MAX(CASE WHEN meta_event = 'Produced_webinar_attended_on_demand' THEN 2 ELSE 0 END) AS Produced_webinar_attended_on_demand,
      MAX(CASE WHEN meta_event = 'Paid_content_syndication_filled_out_form' THEN 1 ELSE 0 END) AS Paid_content_syndication_filled_out_form,
      MAX(CASE WHEN meta_event = 'referral_customer_filled_out_form' THEN 10 ELSE 0 END) AS referral_customer_filled_out_form,
      MAX(CASE WHEN meta_event = 'referral_partner_filled_out_form' THEN 10 ELSE 0 END) AS referral_partner_filled_out_form,
      MAX(CASE WHEN meta_event = 'BOFU_email_nurture_clicked_link' THEN 2 ELSE 0 END) AS BOFU_email_nurture_clicked_link,
      MAX(CASE WHEN meta_event = 'TOFU_email_nurture_clicked_link' THEN 0 ELSE 0 END) AS TOFU_email_nurture_clicked_link, -- 2018-05-10 by Hannah removed point value
      MAX(CASE WHEN meta_event = 'direct_mail_responded' THEN 0 ELSE 0 END) AS direct_mail_responded,  -- 2018-05-10 by Hannah removed point value
      MAX(CASE WHEN meta_event = 'review_site_filled_out_form' THEN 10 ELSE 0 END) AS review_site_filled_out_form,
      MAX(CASE WHEN meta_event = 'referral_employee_filled_out_form' THEN 10 ELSE 0 END) AS referral_employee_filled_out_form,
      MAX(CASE WHEN meta_event = 'BOFU_email_nurture_responded' THEN 10 ELSE 0 END) AS BOFU_email_nurture_responded,
      MAX(CASE WHEN meta_event = 'Produced_webinar_no_show' THEN 1 ELSE 0 END) AS Produced_webinar_no_show,       -- Added 2018-05-10 by Hannah as requested by Henry
      MAX(CASE WHEN meta_event = 'direct_mail_delivered' THEN 0 ELSE 0 END) AS direct_mail_delivered,  -- Added 2018-05-10 by Hannah as requested by Henry
      MAX(CASE WHEN meta_event = 'BOFU_email_nurture_responded' THEN 0 ELSE 0 END) AS event_customer_conference_registered,  -- Added 2018-05-10 by Hannah as requested by Henry
      MAX(CASE WHEN meta_event = 'BOFU_email_nurture_responded' THEN 0 ELSE 0 END) AS event_customer_conference_attended,  -- Added 2018-05-10 by Hannah as requested by Henry
      MAX(DATEDIFF(DAYS, CASE WHEN e.meta_event IN ('field_event_attended',
                                               'field_event_registered',
                                               'sponsered_event_visited_booth_hot',
                                               'sponsered_event_visited_booth_warm',
                                               'sponsered_event_visited_booth_cold',
                                               'BOFU_content_campaign_visits_landing_page',
                                               'BOFU_content_campaign_filled_out_form',
                                               'BOFU_content_campaign_engages_with_content',
                                               'TOFU_content_campaign_visits_landing_page',
                                               'TOFU_content_campaign_filled_out_form',
                                               'TOFU_content_campaign_engages_with_content',
                                               'Sponsored_webinar_registered',
                                               'Sponsored_webinar_attended',
                                               'Sponsored_webinar_attended_on_demand',
                                               'Produced_webinar_registered',
                                               'Produced_webinar_attended',
                                               'Produced_webinar_attended_on_demand',
                                               'paid_content_syndication_filled_out_form',
                                               'referral_customer_filled_out_form',
                                               'referral_partner_filled_out_form',
                                               'BOFU_email_nurture_clicked_link',
                                               'TOFU_email_nurture_clicked_link',
                                               'direct_mail_responded',
                                               'review_site_filled_out_form',
                                               'referral_employee_filled_out_form',
                                               'BOFU_email_nurture_responded',
                                               'Produced_webinar_no_show',
                                               'direct_mail_delivered',
                                               'event_customer_conference_registered',
                                               'event_customer_conference_attended') THEN e.event_timestamp ELSE GETDATE() END, GETDATE())) AS days_since_last_crm_event
FROM
     crm_contacts c
INNER JOIN
     crm_events e
     ON c.id = e.contact_id
WHERE
      DATEDIFF(DAY, e.event_timestamp, GETDATE()) <= 30
GROUP BY
      1
;

-- ===============================
-- Engagement
-- ===============================
DROP TABLE IF EXISTS tmp_analysis_scoring_engagement;
CREATE TEMP TABLE tmp_analysis_scoring_engagement AS
SELECT
			c.contact_id,
			c.source_system,
			c.source_system_object,
			c.source_key_name,
			c.source_key_value,
			c.email,
			c.domain,

			CASE
				WHEN al.mk_account_days_since_last_seen >= 7 THEN -10
				WHEN al.mk_account_days_since_last_seen >= 6 THEN -7
				WHEN al.mk_account_days_since_last_seen >= 5 THEN -5
				WHEN al.mk_account_days_since_last_seen >= 4 THEN -3
				WHEN al.mk_account_days_since_last_seen >= 3 THEN -1
				ELSE 0
			END AS mk_account_days_since_last_seen,

			CASE
				WHEN al.mk_account_number_of_active_days >= 5 THEN 10
				WHEN al.mk_account_number_of_active_days >= 4 THEN 7
				WHEN al.mk_account_number_of_active_days >= 3 THEN 5
				WHEN al.mk_account_number_of_active_days >= 2 THEN 3
				WHEN al.mk_account_number_of_active_days >= 1 THEN 1
				ELSE 0
			END AS mk_account_number_of_active_days,

			CASE
				WHEN mk_account_number_of_demo_requests_last_30_days > 0 THEN 10
				ELSE 0
			END AS mk_account_number_of_demo_requests_last_30_days,

			CASE
				WHEN ad.mk_account_number_of_view_events_last_30_days >= 15 THEN 10
				WHEN ad.mk_account_number_of_view_events_last_30_days >= 10 THEN 7
				WHEN ad.mk_account_number_of_view_events_last_30_days >= 7 THEN 5
				WHEN ad.mk_account_number_of_view_events_last_30_days >= 5 THEN 3
				WHEN ad.mk_account_number_of_view_events_last_30_days >= 3 THEN 1
				ELSE 0
			END AS mk_account_number_of_view_events_last_30_days,

			CASE
				WHEN ad.mk_account_number_of_content_last_30_days >= 5 THEN 10
				WHEN ad.mk_account_number_of_content_last_30_days >= 4 THEN 7
				WHEN ad.mk_account_number_of_content_last_30_days >= 3 THEN 5
				WHEN ad.mk_account_number_of_content_last_30_days >= 2 THEN 3
				WHEN ad.mk_account_number_of_content_last_30_days >= 1 THEN 1
				ELSE 0
			END AS mk_account_number_of_content_last_30_days,

			CASE
				WHEN ad.mk_account_number_of_click_last_30_days >= 5 THEN 10
				WHEN ad.mk_account_number_of_click_last_30_days >= 4 THEN 7
				WHEN ad.mk_account_number_of_click_last_30_days >= 3 THEN 5
				WHEN ad.mk_account_number_of_click_last_30_days >= 2 THEN 3
				WHEN ad.mk_account_number_of_click_last_30_days >= 1 THEN 1
				ELSE 0
			END AS mk_account_number_of_click_last_30_days,

			CASE
				WHEN ad.mk_account_number_of_viewed_careers_page_last_30_days >= 5 THEN -10
				WHEN ad.mk_account_number_of_viewed_careers_page_last_30_days >= 4 THEN -7
				WHEN ad.mk_account_number_of_viewed_careers_page_last_30_days >= 3 THEN -5
				WHEN ad.mk_account_number_of_viewed_careers_page_last_30_days >= 2 THEN -3
				WHEN ad.mk_account_number_of_viewed_careers_page_last_30_days >= 1 THEN -1
				ELSE 0
			END AS mk_account_number_of_viewed_careers_page_last_30_days,

			CASE
				WHEN ad.mk_account_number_of_email_delivered_last_30_days >= 5 THEN 10
				WHEN ad.mk_account_number_of_email_delivered_last_30_days >= 4 THEN 7
				WHEN ad.mk_account_number_of_email_delivered_last_30_days >= 3 THEN 5
				WHEN ad.mk_account_number_of_email_delivered_last_30_days >= 2 THEN 3
				WHEN ad.mk_account_number_of_email_delivered_last_30_days >= 1 THEN 1
				ELSE 0
			END AS mk_account_number_of_email_delivered_last_30_days,

			CASE
				WHEN ad.mk_account_number_of_email_opened_last_30_days >= 5 THEN 10
				WHEN ad.mk_account_number_of_email_opened_last_30_days >= 4 THEN 7
				WHEN ad.mk_account_number_of_email_opened_last_30_days >= 3 THEN 5
				WHEN ad.mk_account_number_of_email_opened_last_30_days >= 2 THEN 3
				WHEN ad.mk_account_number_of_email_opened_last_30_days >= 1 THEN 1
				ELSE 0
			END AS mk_account_number_of_email_opened_last_30_days,

			CASE
				WHEN ad.mk_account_number_of_email_link_clicked_last_30_days >= 5 THEN 10
				WHEN ad.mk_account_number_of_email_link_clicked_last_30_days >= 4 THEN 7
				WHEN ad.mk_account_number_of_email_link_clicked_last_30_days >= 3 THEN 5
				WHEN ad.mk_account_number_of_email_link_clicked_last_30_days >= 2 THEN 3
				WHEN ad.mk_account_number_of_email_link_clicked_last_30_days >= 1 THEN 1
				ELSE 0
			END AS mk_account_number_of_email_link_clicked_last_30_days,

			CASE
				WHEN ad.mk_account_number_of_subscribed_last_30_days >= 5 THEN 10
				WHEN ad.mk_account_number_of_subscribed_last_30_days >= 4 THEN 7
				WHEN ad.mk_account_number_of_subscribed_last_30_days >= 3 THEN 5
				WHEN ad.mk_account_number_of_subscribed_last_30_days >= 2 THEN 3
				WHEN ad.mk_account_number_of_subscribed_last_30_days >= 1 THEN 1
				ELSE 0
			END AS mk_account_number_of_subscribed_last_30_days,

			CASE
				WHEN ad.mk_account_number_of_email_bounced_last_30_days >= 2 THEN -10
				WHEN ad.mk_account_number_of_email_bounced_last_30_days >= 1 THEN -5
				ELSE 0
			END AS mk_account_number_of_email_bounced_last_30_days,

			CASE
				WHEN ad.mk_account_number_of_unsubscribed_last_30_days >= 2 THEN -10
				WHEN ad.mk_account_number_of_unsubscribed_last_30_days >= 1 THEN -5
				ELSE 0
			END AS mk_account_number_of_unsubscribed_last_30_days,

      -- Added 2018-02-28 to include Salesforce Campaigns in L2B computation
      COALESCE(ae.field_event_attended, 0) AS field_event_attended,
      COALESCE(ae.field_event_registered, 0) AS field_event_registered,
      COALESCE(ae.sponsered_event_visited_booth_hot, 0) AS sponsered_event_visited_booth_hot,
      COALESCE(ae.sponsered_event_visited_booth_warm, 0) AS sponsered_event_visited_booth_warm,
      COALESCE(ae.sponsered_event_visited_booth_cold, 0) AS sponsered_event_visited_booth_cold,
      COALESCE(ae.BOFU_content_campaign_visits_landing_page, 0) AS BOFU_content_campaign_visits_landing_page,
      COALESCE(ae.BOFU_content_campaign_filled_out_form, 0) AS BOFU_content_campaign_filled_out_form,
      COALESCE(ae.BOFU_content_campaign_engages_with_content, 0) AS BOFU_content_campaign_engages_with_content,
      COALESCE(ae.TOFU_content_campaign_visits_landing_page, 0) AS TOFU_content_campaign_visits_landing_page,
      COALESCE(ae.TOFU_content_campaign_filled_out_form, 0) AS TOFU_content_campaign_filled_out_form,
      COALESCE(ae.TOFU_content_campaign_engages_with_content, 0) AS TOFU_content_campaign_engages_with_content,
      COALESCE(ae.Sponsored_webinar_registered, 0) AS Sponsored_webinar_registered,
      COALESCE(ae.Sponsored_webinar_attended, 0) AS Sponsored_webinar_attended,
      COALESCE(ae.Sponsored_webinar_attended_on_demand, 0) AS Sponsored_webinar_attended_on_demand,
      COALESCE(ae.Produced_webinar_registered, 0) AS Produced_webinar_registered,
      COALESCE(ae.Produced_webinar_attended, 0) AS Produced_webinar_attended,
      COALESCE(ae.Produced_webinar_attended_on_demand, 0) AS Produced_webinar_attended_on_demand,
      COALESCE(ae.paid_content_syndication_filled_out_form, 0) AS paid_content_syndication_filled_out_form,
      COALESCE(ae.referral_customer_filled_out_form, 0) AS referral_customer_filled_out_form,
      COALESCE(ae.referral_partner_filled_out_form, 0) AS referral_partner_filled_out_form,
      COALESCE(ae.BOFU_email_nurture_clicked_link, 0) AS BOFU_email_nurture_clicked_link,
      COALESCE(ae.TOFU_email_nurture_clicked_link, 0) AS TOFU_email_nurture_clicked_link,
      COALESCE(ae.direct_mail_responded, 0) AS direct_mail_responded,
      COALESCE(ae.review_site_filled_out_form, 0) AS review_site_filled_out_form,
      COALESCE(ae.referral_employee_filled_out_form, 0) AS referral_employee_filled_out_form,
      COALESCE(ae.BOFU_email_nurture_responded, 0) AS BOFU_email_nurture_responded,
      COALESCE(ae.Produced_webinar_no_show, 0) AS Produced_webinar_no_show,
      COALESCE(ae.direct_mail_delivered, 0) AS direct_mail_delivered,
      COALESCE(ae.event_customer_conference_registered, 0) AS event_customer_conference_registered,
      COALESCE(ae.event_customer_conference_attended, 0) AS event_customer_conference_attended,
      ae.days_since_last_crm_event
FROM
			contacts AS c
INNER JOIN
			contacts_to_accounts AS c2a
			ON c.contact_id = c2a.contact_id
LEFT JOIN
			tmp_aggregation_lifetime AS al
			ON c2a.account_id = al.account_id
LEFT JOIN
			tmp_aggregation_l30d AS ad
			ON c2a.account_id = ad.account_id
LEFT JOIN
      tmp_aggregation_crm_event AS ae
      ON ae.email = c.email
;

CREATE TABLE IF NOT EXISTS analysis_scoring_engagement (
			contact_id BIGINT,
			source_system VARCHAR(256),
			source_system_object VARCHAR(256),
			source_key_name VARCHAR(256),
			source_key_value VARCHAR(256),
			email VARCHAR(256),
			domain VARCHAR(256),
			logit NUMERIC(30,5),
			engagement_segment VARCHAR(50),
			created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT GETDATE(),
			updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT GETDATE()
);

TRUNCATE TABLE analysis_scoring_engagement;
INSERT INTO analysis_scoring_engagement (
			contact_id,
			source_system,
			source_system_object,
			source_key_name,
			source_key_value,
			email,
			domain,
			logit,
			engagement_segment
)
WITH cte AS (
  SELECT
    contact_id,
    source_system,
    source_system_object,
    source_key_name,
    source_key_value,
    email,
    domain,
    (mk_account_days_since_last_seen +
    mk_account_number_of_active_days +
    mk_account_number_of_demo_requests_last_30_days +
    mk_account_number_of_view_events_last_30_days +
    mk_account_number_of_content_last_30_days +
    mk_account_number_of_click_last_30_days +
    mk_account_number_of_viewed_careers_page_last_30_days +
    mk_account_number_of_email_opened_last_30_days +
    mk_account_number_of_email_link_clicked_last_30_days +
    mk_account_number_of_subscribed_last_30_days +
    mk_account_number_of_email_bounced_last_30_days +
    mk_account_number_of_unsubscribed_last_30_days ) AS segment_events_total,
    -- Added 2018-02-28 by Hannah to include Salesforce Campaigns in L2B computation
    (field_event_attended +
    field_event_registered +
    sponsered_event_visited_booth_hot +
    sponsered_event_visited_booth_warm +
    sponsered_event_visited_booth_cold +
    BOFU_content_campaign_visits_landing_page +
    BOFU_content_campaign_filled_out_form +
    BOFU_content_campaign_engages_with_content +
    TOFU_content_campaign_visits_landing_page +
    TOFU_content_campaign_filled_out_form +
    TOFU_content_campaign_engages_with_content +
    Sponsored_webinar_registered +
    Sponsored_webinar_attended +
    Sponsored_webinar_attended_on_demand +
    Produced_webinar_registered +
    Produced_webinar_attended +
    Produced_webinar_attended_on_demand +
    paid_content_syndication_filled_out_form +
    referral_customer_filled_out_form +
    referral_partner_filled_out_form +
    BOFU_email_nurture_clicked_link +
    TOFU_email_nurture_clicked_link +
    direct_mail_responded +
    review_site_filled_out_form +
    referral_employee_filled_out_form +
    BOFU_email_nurture_responded +
    Produced_webinar_no_show +
    direct_mail_delivered +
    event_customer_conference_registered +
    event_customer_conference_attended) * EXP(-1.0 * days_since_last_crm_event / 30) -- half life of 90 is arbitrary
    AS sf_events_total
    FROM
      tmp_analysis_scoring_engagement
)
SELECT
			contact_id,
			source_system,
			source_system_object,
			source_key_name,
			source_key_value,
			email,
			domain,
      -- changed by Hannah on 2018-05-09
      -- adding more weight to campaign events as requested by Henry
			(1 * segment_events_total + 3 * sf_events_total) / 4 AS logit,
			NULL as engagement_segment
FROM
			cte
;

-- ===============================
-- Conversion
-- ===============================

DROP TABLE IF EXISTS tmp_analysis_scoring_conversion_free_trial;
CREATE TABLE tmp_analysis_scoring_conversion_free_trial AS
SELECT
			r.contact_id,
			r.logit,
			CASE
				WHEN r.logit >= 15 THEN 'very high'
				WHEN r.logit >= 10 THEN 'high'
				WHEN r.logit >= 5 THEN 'medium'
				ELSE 'low'
			END AS likelihood_to_convert_segment,
			r.engagement_segment AS main_reason,
			GETDATE() AS created_at
FROM
			analysis_scoring_engagement r
;

DROP TABLE IF EXISTS analysis_scoring_conversion_free_trial;
ALTER TABLE tmp_analysis_scoring_conversion_free_trial RENAME TO analysis_scoring_conversion_free_trial;

-- ===============================
-- CRM likelihood to buy calculation
-- For people who only exist in Salesforce
-- ===============================
DROP TABLE IF EXISTS tmp_crm_analysis_scoring_engagement;
CREATE TEMP TABLE tmp_crm_analysis_scoring_engagement AS
SELECT
      c.id,
      MAX(CASE WHEN meta_event = 'field_event_attended' THEN 5 ELSE 0 END) AS field_event_attended,
      MAX(CASE WHEN meta_event = 'field_event_registered' THEN 2 ELSE 0 END) AS field_event_registered,
      MAX(CASE WHEN meta_event = 'sponsered_event_visited_booth_hot' THEN 10 ELSE 0 END) AS sponsered_event_visited_booth_hot,
      MAX(CASE WHEN meta_event = 'sponsered_event_visited_booth_warm' THEN 5 ELSE 0 END) AS sponsered_event_visited_booth_warm,
      MAX(CASE WHEN meta_event = 'sponsered_event_visited_booth_cold' THEN 1 ELSE 0 END) AS sponsered_event_visited_booth_cold,
      MAX(CASE WHEN meta_event = 'BOFU_content_campaign_visits_landing_page' THEN 1 ELSE 0 END) AS BOFU_content_campaign_visits_landing_page,
      MAX(CASE WHEN meta_event = 'BOFU_content_campaign_filled_out_form' THEN  3 ELSE 0 END) AS BOFU_content_campaign_filled_out_form,
      MAX(CASE WHEN meta_event = 'BOFU_content_campaign_engages_with_content' THEN 5 ELSE 0 END) AS BOFU_content_campaign_engages_with_content,
      MAX(CASE WHEN meta_event = 'TOFU_content_campaign_visits_landing_page' THEN 1 ELSE 0 END) AS TOFU_content_campaign_visits_landing_page,
      MAX(CASE WHEN meta_event = 'TOFU_content_campaign_filled_out_form' THEN 2 ELSE 0 END) AS TOFU_content_campaign_filled_out_form,
      MAX(CASE WHEN meta_event = 'TOFU_content_campaign_engages_with_content' THEN 3 ELSE 0 END) AS TOFU_content_campaign_engages_with_content,
      MAX(CASE WHEN meta_event = 'Sponsored_webinar_registered' THEN 2 ELSE 0 END) AS Sponsored_webinar_registered,
      MAX(CASE WHEN meta_event = 'Sponsored_webinar_attended' THEN 3 ELSE 0 END) AS Sponsored_webinar_attended,
      MAX(CASE WHEN meta_event = 'Sponsored_webinar_attended_on_demand' THEN 5 ELSE 0 END) AS Sponsored_webinar_attended_on_demand,
      MAX(CASE WHEN meta_event = 'Produced_webinar_registered' THEN 1 ELSE 0 END) AS Produced_webinar_registered,
      MAX(CASE WHEN meta_event = 'Produced_webinar_attended' THEN 1 ELSE 0 END) AS Produced_webinar_attended,
      MAX(CASE WHEN meta_event = 'Produced_webinar_attended_on_demand' THEN 2 ELSE 0 END) AS Produced_webinar_attended_on_demand,
      MAX(CASE WHEN meta_event = 'Paid_content_syndication_filled_out_form' THEN 1 ELSE 0 END) AS Paid_content_syndication_filled_out_form,
      MAX(CASE WHEN meta_event = 'referral_customer_filled_out_form' THEN 10 ELSE 0 END) AS referral_customer_filled_out_form,
      MAX(CASE WHEN meta_event = 'referral_partner_filled_out_form' THEN 10 ELSE 0 END) AS referral_partner_filled_out_form,
      MAX(CASE WHEN meta_event = 'BOFU_email_nurture_clicked_link' THEN 2 ELSE 0 END) AS BOFU_email_nurture_clicked_link,
      MAX(CASE WHEN meta_event = 'TOFU_email_nurture_clicked_link' THEN 1 ELSE 0 END) AS TOFU_email_nurture_clicked_link,
      MAX(CASE WHEN meta_event = 'direct_mail_responded' THEN 10 ELSE 0 END) AS direct_mail_responded,
      MAX(CASE WHEN meta_event = 'review_site_filled_out_form' THEN 10 ELSE 0 END) AS review_site_filled_out_form,
      MAX(CASE WHEN meta_event = 'referral_employee_filled_out_form' THEN 10 ELSE 0 END) AS referral_employee_filled_out_form,
      MAX(CASE WHEN meta_event = 'BOFU_email_nurture_responded' THEN 10 ELSE 0 END) AS BOFU_email_nurture_responded,
      MAX(CASE WHEN meta_event = 'Produced_webinar_no_show' THEN 1 ELSE 0 END) AS Produced_webinar_no_show,       -- Added 2018-05-10 by Hannah as requested by Henry
      MAX(CASE WHEN meta_event = 'direct_mail_delivered' THEN 0 ELSE 0 END) AS direct_mail_delivered,  -- Added 2018-05-10 by Hannah as requested by Henry
      MAX(CASE WHEN meta_event = 'BOFU_email_nurture_responded' THEN 0 ELSE 0 END) AS event_customer_conference_registered,  -- Added 2018-05-10 by Hannah as requested by Henry
      MAX(CASE WHEN meta_event = 'BOFU_email_nurture_responded' THEN 0 ELSE 0 END) AS event_customer_conference_attended,  -- Added 2018-05-10 by Hannah as requested by Henry
      MAX(DATEDIFF(DAYS, CASE WHEN e.meta_event IN ('field_event_attended',
                                               'field_event_registered',
                                               'sponsered_event_visited_booth_hot',
                                               'sponsered_event_visited_booth_warm',
                                               'sponsered_event_visited_booth_cold',
                                               'BOFU_content_campaign_visits_landing_page',
                                               'BOFU_content_campaign_filled_out_form',
                                               'BOFU_content_campaign_engages_with_content',
                                               'TOFU_content_campaign_visits_landing_page',
                                               'TOFU_content_campaign_filled_out_form',
                                               'TOFU_content_campaign_engages_with_content',
                                               'Sponsored_webinar_registered',
                                               'Sponsored_webinar_attended',
                                               'Sponsored_webinar_attended_on_demand',
                                               'Produced_webinar_registered',
                                               'Produced_webinar_attended',
                                               'Produced_webinar_attended_on_demand',
                                               'paid_content_syndication_filled_out_form',
                                               'referral_customer_filled_out_form',
                                               'referral_partner_filled_out_form',
                                               'BOFU_email_nurture_clicked_link',
                                               'TOFU_email_nurture_clicked_link',
                                               'direct_mail_responded',
                                               'review_site_filled_out_form',
                                               'referral_employee_filled_out_form',
                                               'BOFU_email_nurture_responded',
                                               'Produced_webinar_no_show',
                                               'direct_mail_delivered',
                                               'event_customer_conference_registered',
                                               'event_customer_conference_attended') THEN e.event_timestamp ELSE GETDATE() END, GETDATE())) AS days_since_last_crm_event

FROM
      crm_contacts c
INNER JOIN
      crm_events e
      ON c.id = e.contact_id
WHERE
      DATEDIFF(DAY, e.event_timestamp, GETDATE()) <= 30
GROUP BY
      1
;

DROP TABLE IF EXISTS crm_analysis_scoring_conversion;
CREATE TABLE crm_analysis_scoring_conversion AS
WITH cte_logit AS(
  SELECT
		    id,
		    (COALESCE(field_event_registered, 0) +
		    COALESCE(sponsered_event_visited_booth_hot, 0) +
		    COALESCE(sponsered_event_visited_booth_warm, 0) +
		    COALESCE(sponsered_event_visited_booth_cold, 0) +
		    COALESCE(BOFU_content_campaign_visits_landing_page, 0) +
		    COALESCE(BOFU_content_campaign_filled_out_form, 0) +
		    COALESCE(BOFU_content_campaign_engages_with_content, 0) +
		    COALESCE(TOFU_content_campaign_visits_landing_page, 0) +
		    COALESCE(TOFU_content_campaign_filled_out_form, 0) +
		    COALESCE(TOFU_content_campaign_engages_with_content, 0) +
		    COALESCE(Sponsored_webinar_registered, 0) +
		    COALESCE(Sponsored_webinar_attended, 0) +
				COALESCE(Sponsored_webinar_attended_on_demand, 0) +
		    COALESCE(Produced_webinar_registered, 0) +
		    COALESCE(Produced_webinar_attended, 0) +
		    COALESCE(Produced_webinar_attended_on_demand, 0) +
		    COALESCE(paid_content_syndication_filled_out_form, 0) +
		    COALESCE(referral_customer_filled_out_form, 0) +
		    COALESCE(referral_partner_filled_out_form, 0) +
		    COALESCE(BOFU_email_nurture_clicked_link, 0) +
		    COALESCE(TOFU_email_nurture_clicked_link, 0) +
		    COALESCE(direct_mail_responded, 0) +
		    COALESCE(review_site_filled_out_form, 0) +
		    COALESCE(referral_employee_filled_out_form, 0) +
		    COALESCE(BOFU_email_nurture_responded, 0) +
        COALESCE(Produced_webinar_no_show, 0) +
        COALESCE(direct_mail_delivered, 0) +
        COALESCE(event_customer_conference_registered, 0) +
        COALESCE(event_customer_conference_attended, 0)) * EXP(-1.0 * days_since_last_crm_event / 30) -- half life of 90 is arbitrary
		    AS logit
  FROM
    		tmp_crm_analysis_scoring_engagement
),cte AS (
  SELECT
    id,
    logit * 3 / 4 AS logit -- smoothed to match weight
  FROM
    cte_logit
)
SELECT
		  c.id,
		  c.source_system,
		  c.source_system_object,
		  c.source_key_name,
		  c.source_key_value,
		  l.logit,
		  CASE
		    WHEN l.logit >= 15 THEN 'very high'
		    WHEN l.logit >= 10 THEN 'high'
		    WHEN l.logit >= 5 THEN 'medium'
		    ELSE 'low'
		  END AS likelihood_to_convert_segment,
		  NULL AS main_reason,
		  GETDATE() AS created_at
FROM
		  crm_contacts c
LEFT JOIN
			cte l
			ON c.id = l.id
;







-- LG
--
--
--
--


DROP TABLE IF EXISTS prototype_lead_grade_test;
CREATE TABLE prototype_lead_grade_test AS
WITH combined_scores AS
(
  SELECT
        ca.source_system,
        ca.source_system_object,
        ca.source_key_name,
        ca.source_key_value,
        LOWER(ca.email) AS email,
        LOWER(ca.domain) AS domain,
        COALESCE(cf.customer_fit, 'low') AS mk_customer_fit_segment,
        cf.logit AS mk_customer_fit_score,
        LOWER(COALESCE(ns.likelihood_to_convert_segment, crm.likelihood_to_convert_segment, 'not applicable')) AS mk_likelihood_to_buy_segment,
        COALESCE(ns.logit, crm.logit) AS mk_likelihood_to_buy_score,
        ROW_NUMBER() OVER (PARTITION BY ca.source_system, ca.source_system_object, ca.source_key_name, ca.source_key_value ORDER BY
          CASE LOWER(COALESCE(ns.likelihood_to_convert_segment, crm.likelihood_to_convert_segment, 'not applicable'))
            WHEN 'already paying' THEN 0
            WHEN 'very high' THEN 1
            WHEN 'high' THEN 2
            WHEN 'medium' THEN 3
            WHEN 'low' THEN 4
            ELSE 5
          END,
          CASE COALESCE(cf.customer_fit, 'low')
            WHEN 'very good' THEN 1
            WHEN 'good' THEN 2
            WHEN 'medium' THEN 3
            WHEN 'low' THEN 4
            ELSE 5
          END,
          ns.logit DESC NULLS LAST,
          cf.logit DESC NULLS LAST
        ) AS rk
  FROM
        contacts_all AS ca
  LEFT JOIN
        analysis_scoring_customer_fit AS cf
        ON ca.source_system = cf.source_system
        AND ca.source_system_object = cf.source_system_object
        AND ca.source_key_name = cf.source_key_name
        AND ca.source_key_value = cf.source_key_value
  LEFT JOIN
        contacts AS c
        ON LOWER(ca.email) = LOWER(c.email)
  LEFT JOIN
        analysis_scoring_conversion_free_trial AS ns
        ON c.contact_id = ns.contact_id
  LEFT JOIN
      crm_analysis_scoring_conversion AS crm
      ON ca.source_system = crm.source_system
      AND ca.source_system_object = crm.source_system_object
      AND ca.source_key_name = crm.source_key_name
      AND ca.source_key_value = crm.source_key_value
), scoring_stage AS
(
    SELECT
          cs.source_system,
          cs.source_system_object,
          cs.source_key_name,
          cs.source_key_value,
          cs.email,
          cs.domain,
          cs.mk_customer_fit_segment,
          cs.mk_customer_fit_score,
          cs.mk_likelihood_to_buy_segment,
          cs.mk_likelihood_to_buy_score,
          MIN(COALESCE(cs.mk_likelihood_to_buy_score, 0)) OVER () AS min_ltb_score,
          MAX(COALESCE(cs.mk_likelihood_to_buy_score, 0)) OVER () AS max_ltb_score,
          MIN(COALESCE(cs.mk_customer_fit_score, 0)) OVER () AS min_cfit_score,
          MAX(COALESCE(cs.mk_customer_fit_score, 0)) OVER () AS max_cfit_score,
          NTILE(100) OVER(PARTITION BY cs.mk_likelihood_to_buy_segment, cs.mk_customer_fit_segment ORDER BY COALESCE(cs.mk_likelihood_to_buy_score, 0), COALESCE(cs.mk_customer_fit_score, 0)) AS percentile_within_tile
    FROM
          combined_scores AS cs
    WHERE
          cs.rk = 1
)
SELECT
            source_system,
            source_system_object,
            source_key_name,
            source_key_value,
            email,
            domain,

            mk_customer_fit_segment,
            ROUND(100*((1.000 * ss.mk_customer_fit_score - ss.min_cfit_score) / (ss.max_cfit_score - ss.min_cfit_score))) AS mk_customer_fit_score,
            mk_likelihood_to_buy_segment,
            ROUND(100*((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score))) AS mk_likelihood_to_buy_score,

            CASE
                -- A
                WHEN mk_customer_fit_segment IN ('very good') AND mk_likelihood_to_buy_segment IN ('very high', 'already paying')
                  THEN 'A'
                WHEN mk_customer_fit_segment IN ('good') AND mk_likelihood_to_buy_segment IN ('very high', 'already paying')
                  THEN 'A'
                WHEN mk_customer_fit_segment IN ('medium') AND mk_likelihood_to_buy_segment IN ('very high', 'already paying')
                  THEN 'A'
                WHEN mk_customer_fit_segment IN ('very good') AND mk_likelihood_to_buy_segment IN ('high')
                  THEN 'A'
                WHEN mk_customer_fit_segment IN ('very good') AND mk_likelihood_to_buy_segment IN ('medium')
                  THEN 'A'
                -- B
                WHEN mk_customer_fit_segment IN ('excluded', 'low') AND mk_likelihood_to_buy_segment IN ('very high', 'already paying')
                  THEN 'B'
                WHEN mk_customer_fit_segment IN ('good') AND mk_likelihood_to_buy_segment IN ('high')
                  THEN 'B'
                WHEN mk_customer_fit_segment IN ('medium') AND mk_likelihood_to_buy_segment IN ('high')
                  THEN 'B'
                WHEN mk_customer_fit_segment IN ('very good') AND mk_likelihood_to_buy_segment IN ('low', 'not applicable')
                  THEN 'B'
                -- C
                WHEN mk_customer_fit_segment IN ('good') AND mk_likelihood_to_buy_segment IN ('medium', 'low', 'not applicable')
                  THEN 'C'
                WHEN mk_customer_fit_segment IN ('medium') AND mk_likelihood_to_buy_segment IN ('medium')
                  THEN 'C'
                WHEN mk_customer_fit_segment IN ('excluded', 'low') AND mk_likelihood_to_buy_segment IN ('high')
                  THEN 'C'
                -- D
                WHEN mk_customer_fit_segment IN ('excluded', 'low') AND mk_likelihood_to_buy_segment IN ('medium')
                  THEN 'D'
                WHEN mk_customer_fit_segment IN ('medium') AND mk_likelihood_to_buy_segment IN ('low', 'not applicable')
                  THEN 'D'
                -- E
                WHEN mk_customer_fit_segment IN ('excluded', 'low') AND mk_likelihood_to_buy_segment IN ('low', 'not applicable')
                  THEN  'E'
                ELSE 'E'
            END AS mk_lead_grade,

            CASE
                -- A
                WHEN mk_customer_fit_segment IN ('very good') AND mk_likelihood_to_buy_segment IN ('very high', 'already paying')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 8.0 + 92)
                WHEN mk_customer_fit_segment IN ('good') AND mk_likelihood_to_buy_segment IN ('very high', 'already paying')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 4.0 + 88)
                WHEN mk_customer_fit_segment IN ('medium') AND mk_likelihood_to_buy_segment IN ('very high', 'already paying')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 4.0 + 84)
                WHEN mk_customer_fit_segment IN ('very good') AND mk_likelihood_to_buy_segment IN ('high')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 2.0 + 82)
                WHEN mk_customer_fit_segment IN ('very good') AND mk_likelihood_to_buy_segment IN ('medium')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 2.0 + 80)
                -- B
                WHEN mk_customer_fit_segment IN ('excluded', 'low') AND mk_likelihood_to_buy_segment IN ('very high', 'already paying')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 6.0 + 73)
                WHEN mk_customer_fit_segment IN ('good') AND mk_likelihood_to_buy_segment IN ('high')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 7.0 + 72)
                WHEN mk_customer_fit_segment IN ('medium') AND mk_likelihood_to_buy_segment IN ('high')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 4.0 + 67)
                WHEN mk_customer_fit_segment IN ('very good') AND mk_likelihood_to_buy_segment IN ('low', 'not applicable')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 7.0 + 60)
                -- C
                WHEN mk_customer_fit_segment IN ('good') AND mk_likelihood_to_buy_segment IN ('medium', 'low', 'not applicable')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 9.0 + 50)
                WHEN mk_customer_fit_segment IN ('medium') AND mk_likelihood_to_buy_segment IN ('medium')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 8.0 + 45)
                WHEN mk_customer_fit_segment IN ('excluded', 'low') AND mk_likelihood_to_buy_segment IN ('high')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 8.0 + 40)
                -- D
                WHEN mk_customer_fit_segment IN ('excluded', 'low') AND mk_likelihood_to_buy_segment IN ('medium')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 9.0 + 30)
                WHEN mk_customer_fit_segment IN ('medium') AND mk_likelihood_to_buy_segment IN ('low', 'not applicable')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 9.0 + 20)
                -- E
                WHEN mk_customer_fit_segment IN ('excluded', 'low') AND mk_likelihood_to_buy_segment IN ('low', 'not applicable')
                  THEN ROUND((1.000 * ss.mk_likelihood_to_buy_score - ss.min_ltb_score) / (ss.max_ltb_score - ss.min_ltb_score) * 17.0 + 3)
                ELSE 0
            END AS mk_lead_value,

            GETDATE() AS created_at

FROM
            scoring_stage AS ss
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


