DECLARE today DATE DEFAULT DATE('{{ today }}');


SELECT DISTINCT
    bui.advertising_account_id,
    bui.portfolio_id,
FROM
    `{{ project }}.{{ dataset }}.bidding_unit_info` AS bui
WHERE
    bui.data_date = today
