DECLARE today DATE DEFAULT DATE('{{ today }}');
DECLARE start_from DATE DEFAULT DATE_SUB(today, INTERVAL 60 DAY);

SELECT
    unit.advertising_account_id,
    unit.portfolio_id,
    k.campaign_id,
    'keyword' AS ad_type,
    kq.keyword_id AS ad_id,
    kq.query,
    dkq.date,
    dkq.clicks as query_clicks,
    dkq.conversions as query_conversions,
FROM
    `{{ project }}.{{ commerce_flow_dataset }}.daily_keyword_queries` AS dkq
INNER JOIN
    `{{ project }}.{{ commerce_flow_dataset }}.keyword_queries` AS kq
ON
    dkq.keyword_query_id = kq.id
INNER JOIN
    `{{ project }}.{{ commerce_flow_dataset }}.keywords` AS k
ON
    kq.keyword_id = k.id
INNER JOIN
    `{{ project }}.{{ commerce_flow_dataset }}.campaigns` AS c
ON
    k.campaign_id = c.id
INNER JOIN
    `{{ project }}.{{ dataset }}.bidding_unit_info` AS unit
ON
    c.advertising_account_id = unit.advertising_account_id
    AND (c.portfolio_id = unit.portfolio_id OR (c.portfolio_id IS NULL AND unit.portfolio_id IS NULL))
WHERE
    k.id IS NOT NULL
    AND unit.data_date = today
    {% if is_inference %}
    AND dkq.date BETWEEN start_from AND today
    {% endif %}