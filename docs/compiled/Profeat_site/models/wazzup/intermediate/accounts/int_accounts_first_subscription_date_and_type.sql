with subscriptions as (
select account_id,
    start_date,
    row_number() over (partition by account_id order by start_occured_at asc) rn,
    subscription_id
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_who_paid_missing_data__paidat_and_expiresat_from_eventlogs`),

first_subscription  as (
    select account_id,
    start_date,
    subscription_id
    from subscriptions
    where rn=1
),

subscriptions_parameters as (
    select tariff_new, 
    period_new,
    quantity_new,
    subscription_id,
    row_number() over (partition by subscription_id order by created_at asc) rn ,
    created_date
    from  `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_updates__corect_date_and_filledna_tarif_period_quantity`
    where activation_object in ('payment','partnerBalance','invoice')),

first_subscription_parameters as (
    select * from subscriptions_parameters
    where rn=1
),

subscription_type as (
    select type as subscription_type,
    guid as subscription_id,
    paid_at 
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`
    where paid_at is not null
),

subscription_type_date_parameters as (
    select 
    first_subscription.account_id,                          -- ID аккаунта
    first_subscription_parameters.tariff_new as tariff,     -- Тариф подписки
    first_subscription_parameters.period_new as period,     -- Период подписки
    first_subscription_parameters.quantity_new as quantity, -- Количество каналов в подписке
    start_date,                                             -- Дата начала подписки
    subscription_type.subscription_type,                    -- Транспорт подписки
    paid_at,                                                -- Дата и время оплаты подписки
    first_subscription.subscription_id                      -- ID подписки
    from first_subscription 
    left join first_subscription_parameters 
    on first_subscription.subscription_id=first_subscription_parameters.subscription_id
    left join subscription_type
    on subscription_type.subscription_id=first_subscription.subscription_id
)
    -- Таблица, которая показывает первую подписку на аккаунте с её типом и датой
select * from subscription_type_date_parameters