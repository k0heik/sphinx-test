DECLARE today DATE DEFAULT DATE('{{ today }}');
DECLARE last_month_last_day DEFAULT(DATE_SUB(DATE_TRUNC(today, MONTH), INTERVAL 1 DAY));
DECLARE month_last_day DATE DEFAULT DATE_SUB(DATE_TRUNC(DATE_ADD(today, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY);

SELECT
    DATE(start_date, 'Asia/Tokyo') as start_date,
    DATE(end_date, 'Asia/Tokyo') as end_date,
    coefficient,
    updated_at,
FROM
    `{{ project }}.{{ commerce_flow_dataset }}.daily_budget_boost_coefficients`
WHERE
{% if portfolio_id %}
  unit_type = 'Portfolio' AND unit_id = {{ portfolio_id }}
{% else %}
  unit_type = 'AdvertisingAccount' AND unit_id = {{ advertising_account_id }}
{% endif %}
  AND DATE(end_date, 'Asia/Tokyo') >= last_month_last_day
  AND DATE(start_date, 'Asia/Tokyo') <= month_last_day
