WITH churn AS (
    SELECT *
    FROM `dwh-wazzup`.`dbt_nbespalov`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates`
),

channels_used AS (
    SELECT *
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_active_channel_transport_by_month`
)

SELECT
    churn.*,
    transport
FROM churn
JOIN channels_used ON churn.account_id = channels_used.account_id AND DATE_TRUNC(churn.date, month) = channels_used.month