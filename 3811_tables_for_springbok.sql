SET SEARCH_PATH TO mk_3811_1;


DROP TABLE IF EXISTS tmp_audience;
CREATE TEMP TABLE tmp_audience AS
SELECT
    h.id,
    h.email,
    h.domain,
    c2a.account_id
FROM
    contacts h
LEFT JOIN
    contacts s
    ON LOWER(h.email) = LOWER(s.email)
    AND s.source_system = 'segment'
LEFT JOIN
    contacts_to_accounts c2a
    ON s.contact_id = c2a.contact_id
-- LEFT JOIN
--     accounts_attributes AS at
--     ON at.account_id = c2a.account_id
WHERE
    h.created_date >= '20180301'
    AND h.created_date < '20180601'
    AND h.source_system = 'hubspot'
    -- -- Filter on leads with sales activity (optional)
    -- AND c.a_notes_last_contacted IS NOT NULL

    -- -- Filter on lead source when possible/necessary
    AND h.a_leadsource IN ('Sales', 'Direct', 'Organic', 'Invited user')
;

DROP TABLE IF EXISTS tmp_conversion;
CREATE TEMP TABLE tmp_conversion AS
SELECT
  a.account_id AS account_id
FROM
  tmp_audience c
INNER JOIN
  accounts_attributes a
  ON c.account_id = a.account_id
  AND a.is_paying = true
  AND a.mrr::FLOAT >= 249
GROUP BY
  1
;

SELECT
  COUNT(1),
  COUNT(DISTINCT email),
  COUNT(DISTINCT CASE WHEN tgt.account_id IS NOT NULL THEN tgt.account_id END) AS count_conversions_at_account_level,
  SUM(CASE WHEN tgt.account_id IS NOT NULL THEN 1 ELSE 0 END ) AS count_target
FROM
  tmp_audience c
LEFT JOIN
  tmp_conversion tgt
  ON tgt.account_id = c.account_id
;


-- VALIDATION


DROP TABLE IF EXISTS tmp_audience_val;
CREATE TEMP TABLE tmp_audience_val AS
SELECT
    h.id,
    h.email,
    h.domain,
    c2a.account_id
FROM
    contacts h
LEFT JOIN
    contacts s
    ON LOWER(h.email) = LOWER(s.email)
    AND s.source_system = 'segment'
LEFT JOIN
    contacts_to_accounts c2a
    ON s.contact_id = c2a.contact_id
-- LEFT JOIN
--     accounts_attributes AS at
--     ON at.account_id = c2a.account_id
WHERE
    h.created_date >= '20180101'
    AND h.created_date < '20180301'
    AND h.source_system = 'hubspot'
    -- -- Filter on leads with sales activity (optional)
    -- AND c.a_notes_last_contacted IS NOT NULL

    -- -- Filter on lead source when possible/necessary
    AND h.a_leadsource IN ('Sales', 'Direct', 'Organic', 'Invited user')
;

DROP TABLE IF EXISTS tmp_conversion_val;
CREATE TEMP TABLE tmp_conversion_val AS
SELECT
  a.account_id AS account_id
FROM
  tmp_audience_val c
INNER JOIN
  accounts_attributes a
  ON c.account_id = a.account_id
  AND a.is_paying = true
  AND a.mrr::FLOAT >= 249
GROUP BY
  1
;

-- SMOOTHING


DROP TABLE IF EXISTS tmp_audience_smooth;
CREATE TEMP TABLE tmp_audience_smooth AS
SELECT
    h.id,
    h.email,
    h.domain,
    c2a.account_id
FROM
    contacts h
LEFT JOIN
    contacts s
    ON LOWER(h.email) = LOWER(s.email)
    AND s.source_system = 'segment'
LEFT JOIN
    contacts_to_accounts c2a
    ON s.contact_id = c2a.contact_id
-- LEFT JOIN
--     accounts_attributes AS at
--     ON at.account_id = c2a.account_id
WHERE
    h.created_date >= GETDATE() - 90
    AND h.created_date < GETDATE() - 30
    AND h.source_system = 'hubspot'
    -- -- Filter on leads with sales activity (optional)
    -- AND c.a_notes_last_contacted IS NOT NULL

    -- -- Filter on lead source when possible/necessary
    AND h.a_leadsource IN ('Sales', 'Direct', 'Organic', 'Invited user')
;

DROP TABLE IF EXISTS tmp_conversion_smooth;
CREATE TEMP TABLE tmp_conversion_smooth AS
SELECT
  a.account_id AS account_id
FROM
  tmp_audience_smooth c
INNER JOIN
  accounts_attributes a
  ON c.account_id = a.account_id
  AND a.is_paying = true
  AND a.mrr::FLOAT >= 249
GROUP BY
  1
;

-- Combine
DROP TABLE IF EXISTS analysis_training_dataset;
CREATE TABLE analysis_training_dataset AS
SELECT
    a.email,
    CASE WHEN tgt.account_id IS NOT NULL THEN 1 ELSE 0 END AS target
FROM
      tmp_audience AS a
LEFT JOIN
      tmp_conversion tgt
      ON a.account_id = tgt.account_id
;

-- Combine
DROP TABLE IF EXISTS analysis_validation_dataset;
CREATE TABLE analysis_validation_dataset AS
SELECT
    a.email,
    CASE WHEN tgt.account_id IS NOT NULL THEN 1 ELSE 0 END AS target
FROM
      tmp_audience_val AS a
LEFT JOIN
      tmp_conversion_val tgt
      ON a.account_id = tgt.account_id
;


-- Combine
DROP TABLE IF EXISTS analysis_smoothing_dataset;
CREATE TABLE analysis_smoothing_dataset AS
SELECT
    a.email,
    CASE WHEN tgt.account_id IS NOT NULL THEN 1 ELSE 0 END AS target
FROM
      tmp_audience_smooth AS a
LEFT JOIN
      tmp_conversion_smooth tgt
      ON a.account_id = tgt.account_id
;

SELECT
  target,
  COUNT(1)
FROM
  analysis_smoothing_dataset
GROUP BY 1
;


-- testing current model:

SELECT
  *
FROM
  analysis_scoring_customer_fit
LIMIT 50
;

SELECT
  af.logit,
  COUNT(DISTINCT c.email) AS population,
  SUM(CASE WHEN a.account_id IS NOT NULL THEN 1 ELSE 0 END) AS conversion,
  SUM(CASE WHEN a.account_id IS NOT NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(DISTINCT c.email) conversion_rate
FROM
  tmp_audience c
INNER JOIN
  analysis_scoring_customer_fit af
  ON c.email = af.email
LEFT JOIN
  tmp_conversion a
  ON a.account_id = c.account_id
GROUP BY
  1
ORDER BY
  1 ASC