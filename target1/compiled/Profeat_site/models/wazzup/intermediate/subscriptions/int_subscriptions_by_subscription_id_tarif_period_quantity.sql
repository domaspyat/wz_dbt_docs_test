with combined_intervals as (
    select int_subscription_deduplicated.*,
    billingpackages.type
     from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_deduplicated_by_id_combined_intervals_only_paid` int_subscription_deduplicated
        inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingpackages 
    on billingpackages.guid=int_subscription_deduplicated.subscription_id
    where billingpackages.paid_at is not null 
),

last_value_tarif as (
    select stg_subscriptionUpdates.subscription_id,                         -- ID подписки
    data_source,                                                            -- Способ оплаты: card (оплата по карте), bills (оплата по счёту), partner_payment (оплата партнером) или null
    partner_id,                                                             -- ID партнера, если оплата была партнером
    date_trunc(stg_subscriptionUpdates.paid_date, month) as paid_month,     -- Месяц оплаты
    stg_subscriptionUpdates.paid_date,                                      -- Дата оплаты
    billingpackages.type as subscription_type,                              -- Тип (траснпорт) подписки
    coalesce(last_value(tariff_new) over (partition by stg_subscriptionUpdates.subscription_id, stg_subscriptionUpdates.paid_date order by stg_subscriptionUpdates.paid_date desc), tariff) as tariff_new,          -- Новый тариф подписки
    coalesce(last_value(period_new) over (partition by stg_subscriptionUpdates.subscription_id, stg_subscriptionUpdates.paid_date  order by stg_subscriptionUpdates.paid_date desc), period) as period_new,         -- Новый период подписки
    coalesce(last_value(quantity_new) over (partition by stg_subscriptionUpdates.subscription_id, stg_subscriptionUpdates.paid_date  order by stg_subscriptionUpdates.paid_date desc), quantity) as quantity_new    -- Новое кол-во каналов в подписке
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_updates__corect_date_and_filledna_tarif_period_quantity` stg_subscriptionUpdates
    join  `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_and_wapi_sessions_real_money_with_data_source_and_subscription_update_id` who_paid_data 
                                                                                                    on stg_subscriptionUpdates.guid = who_paid_data.subscription_update_id
    left join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`  billingpackages on billingpackages.guid=stg_subscriptionUpdates.subscription_id
    where tariff_new is not null
)   -- Таблица подписок со способом их оплаты и новым тарифом
select *
from last_value_tarif