DECLARE today DATE DEFAULT DATE('{{ today }}');

WITH unit_info AS (
    SELECT
        pa.advertising_account_id,
        pa.id AS portfolio_id,
        aa.type AS account_type,
        osa.optimization_costs,
        osa.optimization_priority_mode_type,
        osa.optimization_purpose,
        osa.optimization_purpose_value,
    FROM
        `{{ project }}.{{ commerce_flow_dataset }}.portfolios` AS pa
    INNER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.advertising_accounts` AS aa
    ON
        pa.advertising_account_id = aa.id
    INNER JOIN
        `{{ project }}.{{ commerce_flow_dataset }}.optimization_settings` AS osa
    ON
        pa.id = osa.portfolio_id
    WHERE
        pa.advertising_account_id = {{ advertising_account_id }}
    AND
        pa.id = {{ portfolio_id }}
)
SELECT
    ui.advertising_account_id,
    ui.portfolio_id,
    ui.account_type,
    ui.optimization_costs,
    ui.optimization_priority_mode_type,
    ui.optimization_purpose,
    ui.optimization_purpose_value,
    bui.start,
    bui.round_up_point,
FROM
    unit_info ui
INNER JOIN
    `{{ project }}.{{ dataset }}.bidding_unit_info` AS bui
ON
    bui.data_date = today
AND
    ui.advertising_account_id = bui.advertising_account_id
AND
    (
        (ui.portfolio_id = bui.portfolio_id)
        OR (ui.portfolio_id IS NULL AND bui.portfolio_id IS NULL)
    )
