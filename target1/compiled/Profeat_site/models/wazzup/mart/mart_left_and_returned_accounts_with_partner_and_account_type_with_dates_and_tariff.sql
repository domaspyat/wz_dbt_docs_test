WITH churn AS (
    SELECT *
    FROM `dwh-wazzup`.`dbt_nbespalov`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates`
),

tariffs_used AS (
    SELECT *
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_tariffs_by_month`
)

SELECT
    churn.*,
    tariff
FROM churn
JOIN tariffs_used ON churn.account_id = tariffs_used.account_id AND DATE_TRUNC(churn.date, month) = tariffs_used.active_month