DECLARE today DATE DEFAULT DATE('{{ today }}');

WITH targ_ad AS (
    SELECT DISTINCT
        advertising_account_id,
        portfolio_id,
        campaign_id,
        ad_type,
        ad_id,
        is_enabled_bidding_auto_adjustment,
    FROM
        `{{ project }}.{{ dataset }}.bidding_ad_performance`
    WHERE
        data_date = today
    AND
        advertising_account_id = {{ advertising_account_id }}
    {% if portfolio_id %}
    AND
        portfolio_id = {{ portfolio_id }}
    {% else %}
    AND
        portfolio_id IS NULL
    {% endif %}
),
product_targetings AS (
    SELECT
        targ_ad.ad_type,
        targ_ad.ad_id,
        pta.ad_group_id,
        pta.bidding_price,
        '' AS match_type,
    FROM
        targ_ad
    INNER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.product_targetings` AS pta
    ON
        pta.id = targ_ad.ad_id
    AND
        targ_ad.ad_type = "product_targeting"
),
targetings AS (
    SELECT
        targ_ad.ad_type,
        targ_ad.ad_id,
        ta.ad_group_id,
        ta.bidding_price,
        '' AS match_type,
    FROM
        targ_ad
    INNER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.targetings` AS ta
    ON
        ta.id = targ_ad.ad_id
    AND
        targ_ad.ad_type = "targeting"
),
keywords AS (
    SELECT
        targ_ad.ad_type,
        targ_ad.ad_id,
        ka.ad_group_id,
        ka.bidding_price,
        ka.match_type,
    FROM
        targ_ad
    INNER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.keywords` AS ka
    ON
        ka.id = targ_ad.ad_id
    AND
        targ_ad.ad_type = "keyword"
),
tmp_ad_info AS (
    SELECT * FROM product_targetings
    UNION ALL
    SELECT * FROM targetings
    UNION ALL
    SELECT * FROM keywords
),
ad_info AS (
    SELECT
        ad.*,
        COALESCE(ad.bidding_price, ag.default_bid) AS comp_bidding_price,
    FROM
        tmp_ad_info as ad
    LEFT OUTER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.ad_groups` AS ag
    ON
        ad.ad_group_id = ag.id
)
SELECT
    targ_ad.advertising_account_id,
    targ_ad.portfolio_id,
    targ_ad.campaign_id,
    targ_ad.ad_type,
    targ_ad.ad_id,
    ad_info.comp_bidding_price as bidding_price,
    ad_info.match_type,
    targ_ad.is_enabled_bidding_auto_adjustment,
FROM
    targ_ad
INNER JOIN
    ad_info
ON
    targ_ad.ad_type = ad_info.ad_type
AND
    targ_ad.ad_id = ad_info.ad_id
ORDER BY
    1, 2, 3, 4, 5
