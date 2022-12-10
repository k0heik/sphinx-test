DECLARE today DATE DEFAULT DATE('{{ today }}');
DECLARE yesterday DATE DEFAULT DATE_SUB(today, INTERVAL 1 DAY);

SELECT
    ad.ad_type,
    ad.ad_id,
    ad.bidding_price,
FROM
    `{{ project }}.{{ dataset }}.ml_result_unit` unit
INNER JOIN
    `{{ project }}.{{ dataset }}.ml_result_ad` ad
ON
    unit.advertising_account_id = ad.advertising_account_id
AND
    (
        unit.portfolio_id = ad.portfolio_id
        OR (unit.portfolio_id IS NULL AND ad.portfolio_id IS NULL)
    )
AND
    unit.date = ad.date
WHERE
    unit.date = yesterday
AND
    unit.advertising_account_id = {{ advertising_account_id }}
{% if portfolio_id %}
AND
    unit.portfolio_id = {{ portfolio_id }}
{% else %}
AND
    unit.portfolio_id IS NULL
{% endif %}
AND unit.is_ml_enabled
