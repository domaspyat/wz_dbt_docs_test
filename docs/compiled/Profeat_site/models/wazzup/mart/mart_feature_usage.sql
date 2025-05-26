with analytics_usages as (
    select  cast(account_id as integer) as account_id, month,  'analytics_usage' as feature from `dwh-wazzup`.`dbt_nbespalov`.`int_features_analytics_usages_by_month`
),

iframe_usages as (
    select  cast(account_id as integer) as account_id, month, iframe_open_employees , 'iframe_usage' as feature from `dwh-wazzup`.`dbt_nbespalov`.`int_features_iframe_usage_by_month`
),

sstt as (
    select  cast(account_id as integer) as account_id, month,  'sstt' as feature from `dwh-wazzup`.`dbt_nbespalov`.`int_features_messages_sstt`
),

groupchats as (
    select  cast(account_id as integer) as account_id, month,  'groupchats' as feature from `dwh-wazzup`.`dbt_nbespalov`.`int_features_groupchats`
),

notifications as (
    select  cast(account_id as integer) as account_id, month, 'notifications' as feature from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_notifications_active_by_month`
    where active_days>=7
),

net_churn_revenue as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_net_churn_revenue`
),

onboarding as (
    select  cast(account_id as integer) as account_id,
    russian_country_name,
    account_registration_type_current,
    region_international,
    account_currency,
    registration_date,
    date_trunc(registration_date, month) as registration_month
    from `dwh-wazzup`.`dbt_nbespalov`.`mart_onboarding__accounts_integrations_subscriptions_channels_messages`
), 

active_or_left as (
    select account_id, 
    month, 
    is_active, 
    has_left from  `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_active_or_left`
),

retained_second_month as (
   select account_id,
   max(is_retained_second_month) as is_retained_second_month,
   max(date_trunc(first_subscription_start,month)) as first_subscription_start_month
    from   `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_active_or_left`
   group by 1
),

has_max_by_month as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicated_by_id_tariff`
),


features_by_month as (
select coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(analytics_usages.month, iframe_usages.month), sstt.month),groupchats.month), notifications.month), active_or_left.month), has_max_by_month.month), net_churn_revenue.paid_month) as month, 
coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(coalesce(analytics_usages.account_id, iframe_usages.account_id), sstt.account_id),groupchats.account_id), notifications.account_id), active_or_left.account_id), has_max_by_month.account_id), net_churn_revenue.account_id)  as account_id, 
max(case when analytics_usages.account_id is not null then True else False end) as has_analytics_usage,
max(case when iframe_usages.account_id is not null then True else False end)  as has_iframe_usage,
max(case when sstt.account_id is not null then True else False end)  as has_sstt_usage,
max(case when groupchats.account_id is not null then True else False end)  as has_groupchats_usage,
max(case when notifications.account_id is not null then True else False end)  as has_notifications_usage,
max(is_active) as is_active,
max(has_left) as has_left,
max(iframe_open_employees) as iframe_open_employees,
max(has_max_by_month.has_max) as has_max,
max(sum_in_rubles_downsell_loss) as sum_in_rubles_downsell_loss,
max(sum_in_rubles_returned_revenue) as sum_in_rubles_returned_revenue,
max(sum_in_rubles_churn_loss) as sum_in_rubles_churn_loss,
max(sum_in_rubles_new_users_revenue) as sum_in_rubles_new_users_revenue,
max(sum_in_rubles_old_users_new_subscription) as sum_in_rubles_old_users_new_subscription,
max(sum_in_rubles_upsell_revenue) as sum_in_rubles_upsell_revenue
   from analytics_usages
full outer join iframe_usages
on analytics_usages.account_id = iframe_usages.account_id and analytics_usages.month=iframe_usages.month
full outer join sstt
on iframe_usages.account_id = sstt.account_id and iframe_usages.month=sstt.month
full outer join groupchats
on groupchats.account_id = iframe_usages.account_id and iframe_usages.month=groupchats.month
full outer join notifications
on notifications.account_id = iframe_usages.account_id and iframe_usages.month=notifications.month
full outer join active_or_left
on active_or_left.account_id=iframe_usages.account_id and iframe_usages.month=active_or_left.month
full outer join has_max_by_month
on has_max_by_month.account_id=iframe_usages.account_id and has_max_by_month.month= iframe_usages.month
full outer join net_churn_revenue
on net_churn_revenue.account_id=iframe_usages.account_id and iframe_usages.month=net_churn_revenue.paid_month
group by 1,2
),

features_with_account_info as (
    select month, 
    coalesce(features_by_month.account_id, onboarding.account_id) as account_id,
    has_analytics_usage, 
    has_iframe_usage, 
    has_sstt_usage, 
    has_groupchats_usage, 
    has_notifications_usage, 
    is_active, 
    has_left, 
    iframe_open_employees,   
    has_max,
    sum_in_rubles_downsell_loss,
    sum_in_rubles_returned_revenue,
    sum_in_rubles_churn_loss,
    sum_in_rubles_new_users_revenue,
    sum_in_rubles_old_users_new_subscription,
    sum_in_rubles_upsell_revenue,
    russian_country_name,
    account_registration_type_current,
    account_currency,
    registration_date,
    registration_month
    from  features_by_month 
    left join  onboarding
                on features_by_month.account_id=onboarding.account_id
    ),

accounts_active_month_after_registration_month as (
    SELECT account_id FROM features_with_account_info
    where account_registration_type_current is not null
    group by 1
    having max(registration_month)<min(month)
    ),

union_with_registration_info as (
    select * from features_with_account_info
    UNION ALL 
    select  distinct registration_month as month,       -- учетный месяц
    account_id,                                         -- ID аккаунта
    False as has_analytics_usage,                       -- Пользуются аналитикой  в этом месяце - https://www.notion.so/Feature-Usage-269170c780bb42a9ab5c949bc34935ca?pvs=4
    False as has_iframe_usage,                          -- Используют айфрейм  в этом месяце https://www.notion.so/Feature-Usage-17a51441f7c34e568e50c70de900c725?pvs=4
    False as has_sstt_usage,                            -- Пользуются расшифровкой голосовых сообщений в этом месяце - https://www.notion.so/Feature-Usage-5f51c77233604a8ba862219d29318b08?pvs=4
    false as has_groupchats_usage,                      -- Пользуется групповыми чатами в этом месяце - https://www.notion.so/Feature-Usage-743e4d5dd0b8480ea8cb3e055a2177e7?pvs=4
    false as has_notifications_usage,                   -- Пользуется уведомлениями в этом месяце - https://www.notion.so/bfe09d016b2249a8a7816372944b2a6c?pvs=4#11ac57f0e617410991ce03fb93d334d3
    false as is_active,                                 -- Была ли активная оплаченная подписка в этом месяце
    false as has_left,                                  -- Закончилась ли активная подписка у пользователя в этом меcяце
    null as iframe_open_employees,                      -- Количество сотрудников, заходивших в айфрейм
    false as has_max,                                   -- Есть ли активная оплаченная подписка на max в этом месяце
    0 as sum_in_rubles_downsell_loss,                   -- Потерянная выручка в рублях от понижения существующих подписок существующих клиентов https://www.notion.so/4b61bed20364452ab4cd3aac5de09590?pvs=4
    0 as sum_in_rubles_returned_revenue,                -- Выручка от вернувшихся пользователей https://www.notion.so/4b61bed20364452ab4cd3aac5de09590?pvs=4
    0 as sum_in_rubles_churn_loss,                      -- Потерянная выручка от непродленных/удаленных подписок https://www.notion.so/4b61bed20364452ab4cd3aac5de09590?pvs=4
    0 as sum_in_rubles_new_users_revenue,               -- Выручка от новых пользователей https://www.notion.so/4b61bed20364452ab4cd3aac5de09590?pvs=4
    0 as sum_in_rubles_old_users_new_subscription,      -- Выручка от новых подписок существующих пользователей https://www.notion.so/4b61bed20364452ab4cd3aac5de09590?pvs=4
    0 as sum_in_rubles_upsell_revenue,                  -- Выручка от повышение существующих подписок существующих клиентов https://www.notion.so/4b61bed20364452ab4cd3aac5de09590?pvs=4
    russian_country_name,                               -- Название страны на русском языке
    account_registration_type_current,                  -- Тип аккаунта пользователя
    account_currency,                                   -- Текущая валюта пользователя
    registration_date,                                  -- Дата регистрации пользователя
    registration_month                                  -- Месяц регистрации пользователя
    from  features_with_account_info 
    where exists 
    (select account_id from accounts_active_month_after_registration_month 
    where accounts_active_month_after_registration_month.account_id=features_with_account_info.account_id ) 
),

feature_usages_and_profile_info as (

select union_with_registration_info.*,
    is_retained_second_month,           -- True, если у пользователя есть подписка на второй месяц
    first_subscription_start_month      -- Дата начала подписки
    from union_with_registration_info 
    left join retained_second_month 
    on retained_second_month.account_id=union_with_registration_info.account_id
    
    )
,int_accounts_who_paid__defining_abcx_segmentation_type as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_abcx_segmentation_defining_abcx_segmentation_type`
),
profile_info as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
)
    -- Как пользователи используют фичи ваззап по месяцам
 select feature_usages_and_profile_info.*,
       abcx_segment ,       -- ABCX сегментация
       is_employee,         -- True, если это аккаунт сотрудника
       region_international -- Регион
  from feature_usages_and_profile_info
  left join int_accounts_who_paid__defining_abcx_segmentation_type 
                                                on feature_usages_and_profile_info.account_id = int_accounts_who_paid__defining_abcx_segmentation_type.account_id
                                                and feature_usages_and_profile_info.month  =  int_accounts_who_paid__defining_abcx_segmentation_type.live_month
  left join  profile_info on feature_usages_and_profile_info.account_id = profile_info.account_id