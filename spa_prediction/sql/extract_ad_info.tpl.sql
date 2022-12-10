DECLARE today DATE DEFAULT DATE('{{ today }}');

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
product_targetings AS (
    SELECT
        c.advertising_account_id,
        c.portfolio_id,
        pta.campaign_id,
        pta.ad_group_id,
        pta.id AS prodcut_targeting_id,
        '' AS match_type,
        c.campaign_type,
        c.targeting_type,
        c.budget,
        c.budget_type,
        c.account_type,
        c.optimization_purpose
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
        '' AS match_type,
        c.campaign_type,
        c.targeting_type,
        c.budget,
        c.budget_type,
        c.account_type,
        c.optimization_purpose
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
        ka.match_type,
        c.campaign_type,
        c.targeting_type,
        c.budget,
        c.budget_type,
        c.account_type,
        c.optimization_purpose
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
ad_info AS (
    SELECT
        advertising_account_id,
        portfolio_id,
        campaign_id,
        ad_group_id,
        'product_targeting' AS ad_type,
        prodcut_targeting_id AS ad_id,
        match_type,
        campaign_type,
        targeting_type,
        budget,
        budget_type,
        account_type,
        optimization_purpose
    FROM
        product_targetings
    UNION ALL
    SELECT
        advertising_account_id,
        portfolio_id,
        campaign_id,
        ad_group_id,
        'targeting' AS ad_type,
        targeting_id AS ad_id,
        match_type,
        campaign_type,
        targeting_type,
        budget,
        budget_type,
        account_type,
        optimization_purpose
    FROM
        targetings
    UNION ALL
    SELECT
        advertising_account_id,
        portfolio_id,
        campaign_id,
        ad_group_id,
        'keyword' AS ad_type,
        keyword_id AS ad_id,
        match_type,
        campaign_type,
        targeting_type,
        budget,
        budget_type,
        account_type,
        optimization_purpose
    FROM
        keywords
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
    campaign_id,
    ad_group_id,
    ad_type,
    ad_id,
    match_type,
    campaign_type,
    targeting_type,
    budget,
    budget_type,
    account_type,
    optimization_purpose
FROM
    ad_info
WHERE
    ad_group_id IS NOT NULL
ORDER BY
    1, 2, 3, 4, 5, 6