DECLARE today DATE DEFAULT DATE('{{ today }}');
DECLARE yesterday DATE DEFAULT DATE_SUB(today, INTERVAL 1 DAY);
DECLARE start_from DATE DEFAULT DATE_SUB(today, INTERVAL 60 DAY);

SELECT
    c.id as campaign_id,
    'keyword' AS ad_type,
    k.id AS ad_id,
    kq.query,
    dkq.date,
    dkq.clicks as query_clicks,
    dkq.conversions as query_conversions,
FROM
    `{{ project }}.{{ commerce_flow_dataset }}.campaigns` AS c
INNER JOIN
    `{{ project }}.{{ commerce_flow_dataset }}.keywords` AS k
ON
    k.campaign_id = c.id
INNER JOIN
    `{{ project }}.{{ commerce_flow_dataset }}.keyword_queries` AS kq
ON
    k.id = kq.keyword_id
INNER JOIN
    `{{ project }}.{{ commerce_flow_dataset }}.daily_keyword_queries` AS dkq
ON
    dkq.keyword_query_id = kq.id
WHERE
    c.advertising_account_id = {{ advertising_account_id }}
{% if portfolio_id %}
AND
    c.portfolio_id = {{ portfolio_id }}
{% else %}
AND
    c.portfolio_id IS NULL
{% endif %}
    AND dkq.date BETWEEN start_from AND yesterday
