-- Модель показывает активность подписки по периодам с тарифом и транспортом
SELECT DISTINCT 
    subscriptions_old_and_new_end_date_filledna.*, 
    IFNULL(subscriptionUpdates.tariff, FIRST_VALUE(subscriptionUpdates.tariff IGNORE NULLS) OVER (PARTITION BY subscriptionUpdates.subscription_id ORDER BY created_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS tariff,  -- Здесь подтягивается ближайший тариф, если в текущей записи он null. Нужно для старых записей 2021 года
    billingPackages.type AS transport          -- Тип подписки (транспорт)
FROM `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_old_and_new_end_date_filledna` subscriptions_old_and_new_end_date_filledna
JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` subscriptionUpdates ON subscriptions_old_and_new_end_date_filledna.guid = subscriptionUpdates.guid
JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingPackages ON billingPackages.guid = subscriptions_old_and_new_end_date_filledna.subscription_id