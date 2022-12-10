DECLARE today DATE DEFAULT DATE('{{ today }}');

WITH targ_campaigns AS (
    SELECT DISTINCT
        advertising_account_id,
        portfolio_id,
        campaign_id,
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
)
SELECT
    ca.advertising_account_id,
    ca.portfolio_id,
    ca.id AS campaign_id,
    ca.type AS campaign_type,
    ca.targeting_type,
    ca.budget_type,
FROM
    targ_campaigns
INNER JOIN
    `{{ project }}.{{ commerce_flow_dataset }}.campaigns` AS ca
ON
    ca.advertising_account_id =targ_campaigns.advertising_account_id
AND (ca.portfolio_id = targ_campaigns.portfolio_id
    OR (ca.portfolio_id IS NULL AND targ_campaigns.portfolio_id IS NULL)
)
AND
    ca.id = targ_campaigns.campaign_id
ORDER BY
    1, 2, 3
