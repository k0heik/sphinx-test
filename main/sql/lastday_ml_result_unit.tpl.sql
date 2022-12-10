DECLARE today DATE DEFAULT DATE('{{ today }}');
DECLARE yesterday DATE DEFAULT DATE_SUB(today, INTERVAL 1 DAY);

SELECT DISTINCT
    date,
    p,
    q,
    p_kp,
    p_ki,
    p_kd,
    p_error,
    p_sum_error,
    q_kp,
    q_ki,
    q_kd,
    q_error,
    q_sum_error,
    target_cost,
    target_kpi,
FROM
    `{{ project }}.{{ dataset }}.ml_result_unit`
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
ORDER BY 1
