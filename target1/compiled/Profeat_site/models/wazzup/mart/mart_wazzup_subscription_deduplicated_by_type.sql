with  profile_info as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
),
distinct_subscription_by_user as (
                      SELECT distinct billingpackages.account_id,
                      currency,
                      active_integration_name,
                      account_segment_type as account_type,
                      billingpackages.type
                      FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingpackages
                      inner join profile_info  on billingpackages.account_id=profile_info.account_id 
                     where billingpackages.type!='equipment'
                      and billingpackages.state='active' and (is_free is distinct from True) and  (paid_at is not null)   
                      and  is_employee is false                
                    order by account_id, billingpackages.type),

 subscription_added_data as (
        select account_id, 
        currency,
        account_type,
        active_integration_name,

        string_agg(case when type='whatsapp' then 'Whatsapp Web'
        when type='instagram' then 'Instagram'
        when type='tgapi' then 'Telegram Personal'
        when type='telegram' then 'Telegram Bot'
        when type='waba' then 'Waba'
        when type='avito' then 'Авито'
        when type='vk' then 'Вконтакте'
        else type
        end
        , " + ") as subscription_added
        from distinct_subscription_by_user
        group by  1,2,3,4

 )                   
    -- Таблица с указанием всех активных оплаченных подписок у пользователей на данный момент
select  subscription_added,         -- Все активные оплаченные подписки
currency,                           -- Валюта
account_type,                       -- Тип аккаунта
active_integration_name,            -- Тип активной интеграции на данный момент

LENGTH(subscription_added) - LENGTH(REGEXP_REPLACE(subscription_added, r'\+', '')) + 1 as sub_number, -- Количество подписок в subscription_added
count(distinct account_id) as users -- Уникальное количество пользователей

from subscription_added_data
group by 1,2,3,4