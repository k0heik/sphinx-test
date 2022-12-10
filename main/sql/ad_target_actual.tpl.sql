DECLARE today DATE DEFAULT DATE('{{ today }}');
DECLARE yesterday DATE DEFAULT DATE_SUB(today, INTERVAL 1 DAY);
DECLARE start_from DATE DEFAULT DATE_SUB(yesterday, INTERVAL 60 DAY);

WITH targ_ad AS (
    SELECT DISTINCT
        ad_type,
        ad_id
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
daily_keywords_tmp AS (
    SELECT
        'keyword' AS ad_type,
        dka.keyword_id AS ad_id,
        dka.date,
        ka.ad_group_id,
        kh.bidding_price,
        dka.impressions,
        dka.clicks,
        dka.costs,
        dka.conversions,
        dka.sales,
        ROW_NUMBER() OVER (PARTITION BY dka.keyword_id, dka.date ORDER BY kh.reported_at DESC) AS rn
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.daily_keywords` AS dka
    LEFT OUTER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.keywords` AS ka
    ON
        dka.keyword_id = ka.id
    LEFT OUTER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.keyword_histories` AS kh
    ON
        dka.keyword_id = kh.keyword_id
    AND
        dka.date >= DATE(kh.reported_at, 'Asia/Tokyo')
    INNER JOIN
        targ_ad
    ON
        dka.keyword_id = targ_ad.ad_id
    AND
        targ_ad.ad_type = "keyword"
    WHERE
        dka.date BETWEEN start_from AND yesterday
),
daily_product_targetings_tmp AS (
    SELECT
        'product_targeting' AS ad_type,
        dpta.product_targeting_id AS ad_id,
        dpta.date,
        pta.ad_group_id,
        pth.bidding_price,
        dpta.impressions,
        dpta.clicks,
        dpta.costs,
        dpta.conversions,
        dpta.sales,
        ROW_NUMBER() OVER (PARTITION BY dpta.product_targeting_id, dpta.date ORDER BY pth.reported_at DESC) AS rn
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.daily_product_targetings` AS dpta
    LEFT OUTER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.product_targetings` AS pta
    ON
        dpta.product_targeting_id = pta.id
    LEFT OUTER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.product_targeting_histories` AS pth
    ON
        dpta.product_targeting_id = pth.product_targeting_id
    AND
        dpta.date >= DATE(pth.reported_at, 'Asia/Tokyo')
    INNER JOIN
        targ_ad
    ON
        dpta.product_targeting_id = targ_ad.ad_id
    AND
        targ_ad.ad_type = "product_targeting"
    WHERE
        dpta.date BETWEEN start_from AND yesterday
),
daily_targetings_tmp AS (
    SELECT
        'targeting' AS ad_type,
        dta.targeting_id AS ad_id,
        dta.date,
        ta.ad_group_id,
        th.bidding_price,
        dta.impressions,
        dta.clicks,
        dta.costs,
        dta.conversions,
        dta.sales,
        ROW_NUMBER() OVER (PARTITION BY dta.targeting_id, dta.date ORDER BY th.reported_at DESC) AS rn
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.daily_targetings` AS dta
    LEFT OUTER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.targetings` AS ta
    ON
        dta.targeting_id = ta.id
    LEFT OUTER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.targeting_histories` AS th
    ON
        dta.targeting_id = th.targeting_id
    AND
        dta.date >= DATE(th.reported_at, 'Asia/Tokyo')
    INNER JOIN
        targ_ad
    ON
        dta.targeting_id = targ_ad.ad_id
    AND
        targ_ad.ad_type = "targeting"
    WHERE
        dta.date BETWEEN start_from AND yesterday
),
tmp_ad_records AS (
    SELECT * FROM daily_keywords_tmp WHERE rn = 1
    UNION ALL
    SELECT * FROM daily_product_targetings_tmp WHERE rn = 1
    UNION ALL
    SELECT * FROM daily_targetings_tmp WHERE rn = 1
),
comp_bidding_price_ad_records AS (
    SELECT
        ads.*,
        COALESCE(ads.bidding_price, agh.default_bid) AS comp_bidding_price,
        ROW_NUMBER() OVER (PARTITION BY ads.ad_type, ads.ad_id, ads.date ORDER BY agh.reported_at DESC) AS rn_comp
    FROM
        tmp_ad_records as ads
    LEFT OUTER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.ad_group_histories` AS agh
    ON
        ads.ad_group_id = agh.ad_group_id
    AND
        ads.date >= DATE(agh.reported_at, 'Asia/Tokyo')
),
ad_records AS (
    SELECT * FROM comp_bidding_price_ad_records WHERE rn_comp = 1
)
SELECT
    ad_type,
    ad_id,
    date,
    comp_bidding_price AS bidding_price,
    impressions,
    clicks,
    costs,
    conversions,
    sales
FROM
    ad_records
ORDER BY
    1, 2, 3, 4, 5