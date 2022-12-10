DECLARE today DATE DEFAULT DATE('{{ today }}');
DECLARE yesterday DATE DEFAULT DATE_SUB(today, INTERVAL 1 DAY);
DECLARE start_from DATE DEFAULT DATE_SUB(yesterday, INTERVAL 30 DAY);

SELECT
    ca.id AS campaign_id,
    dca.date,
    dca.impressions,
    dca.clicks,
    dca.costs,
    dca.conversions,
    dca.sales,
FROM
    `{{ project }}.{{ commerce_flow_dataset }}.campaigns` AS ca
LEFT OUTER JOIN
    `{{ project }}.{{ commerce_flow_dataset }}.daily_campaigns` AS dca
ON
    ca.id = dca.campaign_id
WHERE
    ca.advertising_account_id = {{ advertising_account_id }}
    {% if portfolio_id %}
    AND
        ca.portfolio_id = {{ portfolio_id }}
    {% else %}
    AND
        ca.portfolio_id IS NULL
    {% endif %}
    AND dca.date BETWEEN start_from AND yesterday
ORDER BY 1, 2, 3, 4
