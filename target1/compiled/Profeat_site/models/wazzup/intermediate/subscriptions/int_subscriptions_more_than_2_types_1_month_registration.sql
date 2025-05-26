with combined_intervals as ( -- Таблица с информацией об интервалах подписки
    select account_id,  -- ID аккаунта
    subscription_id,    -- ID подписки
    subscription_start  -- Дата начала подписки
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_deduplicated_by_id_combined_intervals`
),

billingPackages as ( -- Таблица с информацией о подписках
    select guid,                -- ID подписки 
    type as subscription_type   -- Тип подписки
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` 
), 

profile_info as (   -- Таблица с информацией об аккаунте
    select account_id,          -- ID аккаунта
    register_date               -- Дата регистрации
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
)
select combined_intervals.account_id as accounts_with_2_or_more_subscription_type   -- Таблица с аккаунтами, у которых 2 или более подписок разных типов
from combined_intervals inner join billingPackages
on combined_intervals.subscription_id=billingPackages.guid
inner join  profile_info
on profile_info.account_id=combined_intervals.account_id
where combined_intervals.subscription_start<=date_add(profile_info.register_date, interval 1 month)
group by 1 
having count (distinct subscription_type)>=2