DECLARE today DATE DEFAULT DATE('{{ today }}');
DECLARE yesterday DATE DEFAULT DATE_SUB(today, INTERVAL 1 DAY);

SELECT DISTINCT
    campaign_id,
    date,
    cap_daily_budget_weight,
FROM
    `{{ project }}.{{ dataset }}.ml_result_campaign`
WHERE
    date = yesterday
AND
    advertising_account_id = {{ advertising_account_id }}
{% if portfolio_id %}
AND
    portfolio_id = {{ portfolio_id }}
{% else %}
AND
    portfolio_id IS NULL
{% endif %}
ORDER BY 1, 2
