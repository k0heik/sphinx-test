DECLARE today DATE DEFAULT DATE('{{ today }}');
DECLARE start_from DATE DEFAULT DATE_SUB(today, INTERVAL {{ num_days_ago }} DAY);

WITH ad_unit AS (
    SELECT
        advertising_account_id,
        portfolio_id
    FROM
        `{{ project }}.{{ dataset }}.bidding_unit_info`
    WHERE
        data_date = today
),
ad AS (
    SELECT
        advertising_account_id,
        portfolio_id,
        campaign_id,
        ad_type,
        ad_id
    FROM
        `{{ project }}.{{ dataset }}.bidding_ad_performance`
    WHERE
        data_date = today
    GROUP BY
        1, 2, 3, 4, 5
),
portfolios AS (
    SELECT
        pa.id AS portfolio_id,
        pa.advertising_account_id,
        aa.type AS account_type,
        osa.optimization_purpose
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.portfolios` AS pa
    LEFT OUTER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.advertising_accounts` AS aa
    ON
        pa.advertising_account_id = aa.id
    LEFT OUTER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.optimization_settings` AS osa
    ON
        pa.id = osa.portfolio_id
    INNER JOIN
        ad_unit
    ON
        pa.advertising_account_id = ad_unit.advertising_account_id
    AND
        pa.id = ad_unit.portfolio_id
),
campaigns AS (
    SELECT
        ca.id AS campaign_id,
        ca.advertising_account_id,
        ca.portfolio_id,
        p.account_type,
        p.optimization_purpose,
        ca.type AS campaign_type,
        ca.targeting_type,
        ca.budget,
        ca.budget_type
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.campaigns` AS ca
    LEFT OUTER JOIN
        portfolios AS p
    ON
        p.portfolio_id = ca.portfolio_id
    WHERE
        p.portfolio_id IS NOT NULL
),
ad_groups AS (
    SELECT
        c.advertising_account_id,
        c.portfolio_id,
        aga.campaign_id,
        aga.id AS ad_group_id
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.ad_groups` AS aga
    LEFT OUTER JOIN
        campaigns AS c
    ON
        aga.campaign_id = c.campaign_id
    WHERE
        c.campaign_id IS NOT NULL
    AND
        EXISTS(
            SELECT
                ad_id
            FROM
                ad
            WHERE
                aga.campaign_id = ad.campaign_id
            AND
                ad.ad_type = "ad_group"
            AND
                aga.id = ad.ad_id
        )
),
product_targetings AS (
    SELECT
        c.advertising_account_id,
        c.portfolio_id,
        pta.campaign_id,
        pta.ad_group_id,
        pta.id AS prodcut_targeting_id,
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.product_targetings` AS pta
    LEFT OUTER JOIN
        campaigns AS c
    ON
        pta.campaign_id = c.campaign_id
    WHERE
        c.campaign_id IS NOT NULL
    AND
        EXISTS(
            SELECT
                ad_id
            FROM
                ad
            WHERE
                pta.campaign_id = ad.campaign_id
            AND
                ad.ad_type = "product_targeting"
            AND
                pta.id = ad.ad_id
        )
),
targetings AS (
    SELECT
        c.advertising_account_id,
        c.portfolio_id,
        ta.ad_group_id,
        ta.campaign_id,
        ta.id AS targeting_id,
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.targetings` AS ta
    LEFT OUTER JOIN
        campaigns AS c
    ON
        ta.campaign_id = c.campaign_id
    WHERE
        c.campaign_id IS NOT NULL
    AND
        EXISTS(
            SELECT
                ad_id
            FROM
                ad
            WHERE
                ta.campaign_id = ad.campaign_id
            AND
                ad.ad_type = "targeting"
            AND
                ta.id = ad.ad_id
        )
),
keywords AS (
    SELECT
        c.advertising_account_id,
        c.portfolio_id,
        ka.campaign_id,
        ka.ad_group_id,
        ka.id AS keyword_id,
        ka.match_type
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.keywords` AS ka
    LEFT OUTER JOIN
        campaigns AS c
    ON
        ka.campaign_id = c.campaign_id
    WHERE
        c.campaign_id IS NOT NULL
    AND
        EXISTS(
            SELECT
                ad_id
            FROM
                ad
            WHERE
                ka.campaign_id = ad.campaign_id
            AND
                ad.ad_type = "keyword"
            AND
                ka.id = ad.ad_id
        )
),
daily_keywords AS (
    SELECT
        c.advertising_account_id,
        c.portfolio_id,
        k.ad_group_id,
        dka.campaign_id,
        'keyword' AS ad_type,
        dka.keyword_id AS ad_id,
        dka.date,
        dka.impressions,
        dka.clicks,
        dka.costs,
        dka.conversions,
        dka.sales
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.daily_keywords` AS dka
    INNER JOIN
        campaigns AS c
    ON
        dka.campaign_id = c.campaign_id
    INNER JOIN
        keywords AS k
    ON
        dka.keyword_id = k.keyword_id
    WHERE
        k.keyword_id IS NOT NULL
    ORDER BY
        1, 2, 3, 4, 5, 6, 7
),
daily_product_targetings AS (
    SELECT
        c.advertising_account_id,
        c.portfolio_id,
        pt.ad_group_id,
        c.campaign_id,
        'product_targeting' AS ad_type,
        dpta.product_targeting_id AS ad_id,
        dpta.date,
        dpta.impressions,
        dpta.clicks,
        dpta.costs,
        dpta.conversions,
        dpta.sales
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.daily_product_targetings` AS dpta
    INNER JOIN
        product_targetings AS pt
    ON
        dpta.product_targeting_id = pt.prodcut_targeting_id
    INNER JOIN
        campaigns AS c
    ON
        pt.campaign_id = c.campaign_id
    WHERE
        pt.prodcut_targeting_id IS NOT NULL
    ORDER BY
        1, 2, 3, 4, 5, 6, 7
),
daily_targetings AS (
    SELECT
        c.advertising_account_id,
        c.portfolio_id,
        t.ad_group_id,
        c.campaign_id,
        'targeting' AS ad_type,
        dta.targeting_id AS ad_id,
        dta.date,
        dta.impressions,
        dta.clicks,
        dta.costs,
        dta.conversions,
        dta.sales
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.daily_targetings` AS dta
    INNER JOIN
        targetings AS t
    ON
        dta.targeting_id = t.targeting_id
    INNER JOIN
        campaigns AS c
    ON
        t.campaign_id = c.campaign_id
    WHERE
        t.targeting_id IS NOT NULL
    ORDER BY
        1, 2, 3, 4, 5, 6, 7
),
daily_ad_groups AS (
    SELECT
        c.advertising_account_id,
        c.portfolio_id,
        ag.ad_group_id,
        c.campaign_id,
        'ad_group' AS ad_type,
        daga.ad_group_id AS ad_id,
        daga.date,
        daga.impressions,
        daga.clicks,
        daga.costs,
        daga.conversions,
        daga.sales,
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.daily_ad_groups` AS daga
    INNER JOIN
        ad_groups AS ag
    ON
        daga.ad_group_id = ag.ad_group_id
    INNER JOIN
        campaigns AS c
    ON
        ag.campaign_id = c.campaign_id
    ORDER BY
        1, 2, 3, 4, 5, 6, 7
),
records AS (
SELECT
    advertising_account_id,
    portfolio_id,
    ad_group_id,
    campaign_id,
    ad_type,
    ad_id,
    date,
    impressions,
    clicks,
    costs,
    conversions,
    sales
FROM
    daily_keywords
UNION ALL
SELECT
    advertising_account_id,
    portfolio_id,
    ad_group_id,
    campaign_id,
    ad_type,
    ad_id,
    date,
    impressions,
    clicks,
    costs,
    conversions,
    sales
FROM
    daily_product_targetings
UNION ALL
SELECT
    advertising_account_id,
    portfolio_id,
    ad_group_id,
    campaign_id,
    ad_type,
    ad_id,
    date,
    impressions,
    clicks,
    costs,
    conversions,
    sales
FROM
    daily_targetings
UNION ALL
SELECT
    advertising_account_id,
    portfolio_id,
    ad_group_id,
    campaign_id,
    ad_type,
    ad_id,
    date,
    impressions,
    clicks,
    costs,
    conversions,
    sales
FROM
    daily_ad_groups
)
SELECT
    CASE
        WHEN portfolio_id IS NULL THEN FARM_FINGERPRINT(
            CONCAT("advertising_account_id_", CAST(advertising_account_id AS STRING)))
        ELSE FARM_FINGERPRINT(
            ARRAY_TO_STRING(
                ["advertising_account_id",
                 CAST(advertising_account_id AS STRING),
                 "portfolio_id",
                 CAST(portfolio_id AS STRING)
                ], '_'))
    END as uid,
    advertising_account_id,
    portfolio_id,
    ad_group_id,
    campaign_id,
    ad_type,
    ad_id,
    date,
    impressions,
    clicks,
    costs,
    conversions,
    sales
FROM
    records
WHERE
    ad_group_id IS NOT NULL AND date >= start_from
ORDER BY
    uid, ad_type, ad_id, date