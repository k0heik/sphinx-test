DECLARE today DATE DEFAULT DATE('{{ today }}');
DECLARE start_from DATE DEFAULT DATE_SUB(today, INTERVAL 60 DAY);

WITH unit AS (
    SELECT
        advertising_account_id,
        portfolio_id
    FROM
        `{{ project }}.{{ dataset }}.bidding_unit_info`
    WHERE
        data_date = today
),
campaigns AS (
    SELECT DISTINCT
        c.id AS campaign_id,
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.campaigns` AS c
    INNER JOIN
        unit
    ON
        unit.advertising_account_id = c.advertising_account_id
        AND (unit.portfolio_id = c.portfolio_id OR (unit.portfolio_id IS NULL AND c.portfolio_id IS NULL))
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
{% if is_inference %}
WHERE
    dcp.date BETWEEN start_from AND today
{% endif %}
