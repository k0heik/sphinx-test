
DECLARE today DATE DEFAULT DATE('{{ today }}');
DECLARE start_from DATE DEFAULT DATE_SUB(today, INTERVAL {{ ml_lookup_days - 1}} DAY);
DECLARE end_to DATE DEFAULT DATE_SUB(today, INTERVAL 1 DAY);

SELECT DISTINCT
    -- わかりやすく「MLが適用されていた日付（レコード日付の前日日付）」を示すため-1日する
    DATE_SUB(date, INTERVAL 1 DAY) AS date,
FROM
    `{{ project }}.{{ dataset }}.ml_result_unit`
WHERE
    date BETWEEN start_from AND end_to
AND
    advertising_account_id = {{ advertising_account_id }}
{% if portfolio_id %}
AND
    portfolio_id = {{ portfolio_id }}
{% else %}
AND
    portfolio_id IS NULL
{% endif %}
AND
    is_lastday_ml_applied
ORDER BY
    1
