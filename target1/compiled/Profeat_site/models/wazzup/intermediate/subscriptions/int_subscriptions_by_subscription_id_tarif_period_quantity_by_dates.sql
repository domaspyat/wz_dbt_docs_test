with combined_intervals as (
    select int_subscription_deduplicated.*,
    billingpackages.type                            -- Тип подписки
     from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_deduplicated_by_id_combined_intervals_only_paid` int_subscription_deduplicated
        inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingpackages 
    on billingpackages.guid=int_subscription_deduplicated.subscription_id
    where billingpackages.paid_at is not null 
),

subscription_with_dates  as (
    select distinct
    date,                                           -- Дата оплаты
    subscription_id,                                -- ID подписки
    account_id,                                     -- ID аккаунта
    LAST_DAY(date, MONTH)  as last_day_of_month,    -- Последний день месяца
    last_value(date) over (partition by date_trunc(date,month),subscription_id order by date asc rows between unbounded preceding and unbounded following ) as last_value_date_by_month -- Последний день месяца по группе 'месяц + ID подписки'
    from combined_intervals inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_days` days
    on days.date>=combined_intervals.subscription_start and days.date<=combined_intervals.subscription_end
    ),

last_value_tarif as (
    select subscription_id,                         -- ID подписки
    paid_date,                                      -- Дата оплаты
    billingpackages.type as subscription_type,      -- Тип (транспорт) подписки
    coalesce(last_value(tariff_new) over (partition by subscription_id, paid_date order by paid_date desc), tariff) as tariff_new,      -- Новый тариф подписки
    coalesce(last_value(period_new) over (partition by subscription_id, paid_date order by paid_date desc), period) as period_new,      -- Новый период подписки
    coalesce(last_value(quantity_new) over (partition by subscription_id, paid_date order by paid_date desc), quantity) as quantity_new -- Новое кол-во каналов в подписке
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_updates__corect_date_and_filledna_tarif_period_quantity` stg_subscriptionUpdates
    left join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`  billingpackages 
    on billingpackages.guid=stg_subscriptionUpdates.subscription_id
),

last_value_tarif_by_subscription_id as (
    select subscription_id,                         -- ID подписки
    subscription_type,                              -- Тип (транспорт) подписки
    paid_date,                                      -- Дата оплаты
    tariff_new,                                     -- Новый тариф подписки
    period_new,                                     -- Новый период подписки
    quantity_new                                    -- Новое кол-во каналов в подписке
    from last_value_tarif  
    group by 1,2,3,4,5,6
),


last_value_tarif_by_subscription_id_next_paid_date as (
    select *,
    coalesce(date_sub(lag(paid_date) over (partition by subscription_id order by paid_date desc),interval 1 day),
    current_date) as next_paid_date
     from last_value_tarif_by_subscription_id
),

subscription_with_date_filter as (
    select * from subscription_with_dates
    where date<=current_date()
),

tarif_info as (
    select subscription_with_date_filter.*, 
    coalesce(tariff_new,tariff) as tariff_new,                               -- Новый тариф подписки
    coalesce(period_new,period) as period_new,                               -- Новый период подписки
    coalesce(quantity_new, quantity) as quantity_new,                        -- Новое кол-во каналов в подписке
    coalesce(subscription_type,billingpackages.type) as subscription_type    -- Тип (транспорт) подписки
     from subscription_with_date_filter
    left join last_value_tarif_by_subscription_id_next_paid_date
    on subscription_with_date_filter.subscription_id=last_value_tarif_by_subscription_id_next_paid_date.subscription_id
    and subscription_with_date_filter.date>=last_value_tarif_by_subscription_id_next_paid_date.paid_date and 
    subscription_with_date_filter.date<=last_value_tarif_by_subscription_id_next_paid_date.next_paid_date
    left join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`  billingpackages 
    on billingpackages.guid=subscription_with_date_filter.subscription_id
    )
        -- Таблица с подписками и их датами с информацией о новом тарифе
select * 
from tarif_info