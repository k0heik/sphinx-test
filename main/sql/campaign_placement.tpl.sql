DECLARE today DATE DEFAULT DATE('{{ today }}');
DECLARE yesterday DATE DEFAULT DATE_SUB(today, INTERVAL 1 DAY);
DECLARE start_from DATE DEFAULT DATE_SUB(today, INTERVAL 60 DAY);

WITH campaigns AS (
    SELECT DISTINCT
        c.id AS campaign_id,
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.campaigns` AS c
    WHERE
        c.advertising_account_id = {{ advertising_account_id }}
    {% if portfolio_id %}
    AND
        c.portfolio_id = {{ portfolio_id }}
    {% else %}
    AND
        c.portfolio_id IS NULL
    {% endif %}
)
SELECT
    cp.campaign_id,
    cp.predicate,
    dcp.date,
    dcp.costs as costs,
    dcp.clicks as clicks,
    dcp.impressions as impressions,
    dcp.sales as sales,
    dcp.conversions as conversions,
FROM
    `{{ project }}.{{ commerce_flow_dataset }}.daily_campaign_placements` AS dcp
INNER JOIN
    `{{ project }}.{{ commerce_flow_dataset }}.campaign_placements` AS cp
ON
    dcp.campaign_placement_id = cp.id
INNER JOIN
    campaigns AS c
ON
    c.campaign_id = cp.campaign_id
WHERE
    dcp.date BETWEEN start_from AND yesterday
