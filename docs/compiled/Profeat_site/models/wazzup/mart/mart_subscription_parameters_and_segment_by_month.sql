with subscription_with_parameters as (
     select *, last_value(tariff_new) over (partition by account_id, subscription_id, date_trunc(date,month) order by date asc rows between unbounded preceding and unbounded following ) as last_value_tarif_month,
    last_value(quantity_new) over (partition by account_id, subscription_id, date_trunc(date,month) order by date asc rows between unbounded preceding and unbounded following) as last_value_quantity_month,
    last_value(period_new) over (partition by account_id, subscription_id, date_trunc(date,month) order by date asc rows between unbounded preceding and unbounded following) as last_value_period_month
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_by_subscription_id_tarif_period_quantity_by_dates`),


wapi_sessions_real_money as (
    select paid_date,
    account_id,
    subscription_id,
    sum(sum_in_rubles_spent_on_subscription) as sum_in_rubles_spent_on_subscription
     from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_and_wapi_sessions_real_money`
     group by 1,2,3

 ),

 subscription_with_parameters_last_value_by_tarif as (
    select date,
    account_id,
    subscription_id,
    last_value_tarif_month,
    last_value_quantity_month,
    last_value_period_month,
    subscription_type,
    last_value_date_by_month
    from subscription_with_parameters
    group by 1,2,3,4,5,6,7,8
 ),

mart_active_accounts_by_date_to_deduplicate as (
  select *,
  row_number() over (partition by account_id, date order by subscription_start desc) as rn_segment
  
   from   `dwh-wazzup`.`dbt_nbespalov`.`mart_active_accounts_by_days_by_segment`
 ),


 active_accounts_by_date_deduplicated as (
    select *,  last_value(segment ignore nulls) over (partition by account_id, date_trunc(date,month) order by date asc rows between unbounded preceding and unbounded following) as last_value_segment_month
     from mart_active_accounts_by_date_to_deduplicate
    where rn_segment=1
), 



 tarif_info_aggregated as (
select subscription_with_parameters.*,
last_value_segment_month,
(case when last_value_segment_month in ('of_partner_child__of_partner_paid') then 'of_partner'
when last_value_segment_month in ('tech_partner_child__child_paid','tech_partner_child__tech_partner_paid') then 'tech_partner'
when last_value_segment_month in ('standart_without_partner','of_partner_child_child_paid') then 'final_clients'
end) as segment_aggregated,
 last_value(segment ignore nulls) over (partition by accounts_by_days_by_segment.account_id, date_trunc(accounts_by_days_by_segment.date,month) order by accounts_by_days_by_segment.date asc rows between unbounded preceding and unbounded following) as last_value_segment_month,
coalesce(sum_in_rubles_spent_on_subscription,0) as sum_in_rubles_spent_on_subscription,
currency
from subscription_with_parameters
left join wapi_sessions_real_money
on subscription_with_parameters.account_id=wapi_sessions_real_money.account_id 
and wapi_sessions_real_money.paid_date=subscription_with_parameters.date
and wapi_sessions_real_money.subscription_id=subscription_with_parameters.subscription_id
left join active_accounts_by_date_deduplicated  accounts_by_days_by_segment 
on accounts_by_days_by_segment.date=subscription_with_parameters.date
and accounts_by_days_by_segment.account_id=subscription_with_parameters.account_id
where segment is not null),


active_channels_with_tarifs as (
  select mart_active_channels.channel_id,
  cast(mart_active_channels.date as date) as date,
  subscription_id
   from  `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_channels` mart_active_channels
),


 channels_count as (SELECT uniquechats_data.channel_id, 
account_id,
 uniquechats_data.date,
 subscription_id,
 unique_chats
 FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_unique_chats` uniquechats_data
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_channels` channels 
on  uniquechats_data.channel_id=channels.guid
left join active_channels_with_tarifs 
on active_channels_with_tarifs.date=uniquechats_data.date
and active_channels_with_tarifs.channel_id=uniquechats_data.channel_id
),

subscription_id_with_unique_chats_count as (
select date_trunc(date,month) as dialog_month,
account_id,
subscription_id,
sum(unique_chats) as unique_chats
from channels_count
 group by 1,2,3),

subscription_parameters_aggregated_by_month as (
select date_trunc(date, month) as subscription_month,                               -- Месяц активности подписки
 segment_aggregated,                                                                -- Сегмент после группировки
 last_value_period_month as period_new,                                             -- Новый период подписки
 last_value_quantity_month as quantity_new,                                         -- Новое кол-во каналов в подписке
 last_value_tarif_month as tariff_new,                                              -- Новый тариф подписки
 subscription_id,                                                                   -- ID подписки
 subscription_type,                                                                 -- Тип (транспорт) подписки
 account_id,                                                                        -- ID аккаунта
 currency,                                                                          -- Валюта
 sum(sum_in_rubles_spent_on_subscription) as sum_in_rubles_spent_on_subscription,   -- Сумма в рублях, потраченная на подписку
 max(last_value_date_by_month) as last_value_date_by_month                          -- Последний день месяца в месяце активности подписки
 from tarif_info_aggregated
 group by 1,2,3,4,5,6,7,8,9),

 subscription_parameters_with_dialogs_by_month as (

 select subscription_parameters_aggregated_by_month.*,
 unique_chats                                                                       -- Количество уникальных диалогов
  from subscription_parameters_aggregated_by_month
 left join subscription_id_with_unique_chats_count 
 on subscription_parameters_aggregated_by_month.subscription_month=subscription_id_with_unique_chats_count.dialog_month 
 and subscription_parameters_aggregated_by_month.subscription_id=subscription_id_with_unique_chats_count.subscription_id)
    -- Параметры подписки и сегмент клиента по месяцам
 select 
* from subscription_parameters_with_dialogs_by_month