SET SEARCH_PATH TO mk_3674_1;

--
-- High Potential Signup
--

DROP TABLE IF EXISTS temp_high_potential_2;
CREATE TEMP TABLE temp_high_potential_2 AS
SELECT
--     c.id AS crm_id,
    c.email AS email,
    obs.company__name,
    obs.domain,
    c.mk_customer_fit AS customer_fit,

    obs.company__metrics__employees AS employee,
    COALESCE(obs.person__employment__title, obs.person__employment__role) AS role,
    COALESCE(obs.company__category__industry, obs.company__category__industry_group, obs.company__category__sector) AS industry,
    COALESCE(obs.company__geo__country, obs.person__geo__country) AS country,
    obs.company__tags,
    obs.company__type,
    obs.company__founded_year,
    obs.company__geo__gdp_per_capita,
    obs.company__metrics__market_cap,
    obs.company__metrics__alexa_rank_global,
    c.created_at

FROM
    export_contacts AS c
LEFT JOIN
    observations_firmographics as obs
    ON c.source_system = obs.source_system
    AND c.source_key_name = obs.source_key_name
    AND c.source_key_value = obs.source_key_value
WHERE
  c.created_at > '2018-01-01'
;


-- Quality
SELECT DISTINCT
		*
FROM
		temp_high_potential_2
WHERE
		customer_fit = 'good'
    AND company__name IS NULL
ORDER BY
		created_at DESC
LIMIT 200;


