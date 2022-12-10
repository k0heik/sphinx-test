DECLARE today DATE DEFAULT DATE('{{ today }}');

SELECT
    campaign_id,
    ad_type,
    ad_id,
    date,
    bidding_price,
    daily_budget,
    minimum_bidding_price,
    maximum_bidding_price,
    minimum_daily_budget,
    maximum_daily_budget,
    is_enabled_daily_budget_auto_adjustment,
    is_enabled_bidding_auto_adjustment,
    impressions,
    clicks,
    conversions,
    sales,
    costs,
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
ORDER BY
    campaign_id,
    ad_type,
    ad_id,
    date
