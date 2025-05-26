WITH subscriptions AS (
                      SELECT *
                      FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_and_wapi_sessions_real_money`
                      )
-- Таблица с действиями с подпиской WABA и отслеживанием сколько было потрачено реальных денег
SELECT subscriptions.*
FROM subscriptions
    LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billing_packages
ON billing_packages.guid=subscriptions.subscription_id
WHERE billing_packages.type='waba'