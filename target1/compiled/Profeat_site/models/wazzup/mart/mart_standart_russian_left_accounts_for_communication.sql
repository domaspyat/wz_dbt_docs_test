with left_accounts as (
    select distinct account_id,
                    subscription_end as data_otvala
    from `dwh-wazzup`.`dbt_nbespalov`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates`
    where return_or_left_status_with_churn_period_7  = 'left'
),   profile_info as (
        select *
        from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
),active_at_the_moment as (
    select distinct account_id
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`
    where state = 'active' and paid_At is not null
)/*, 

has_active_free_subscription as (
    select distinct account_id
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`
    where state = 'active' and is_free = True
)*/
,
 affiliates as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates`
)   -- Таблица с датами отвалившихся обычных пользователей (без партнера).
select  
        left_accounts.account_id,   -- ID аккаунта
        left_accounts.data_otvala,  -- Дата отвала клиента
        profile_info.currency,      -- Валюта
        phone,                      -- телефон пользователя, указанный при регистрации
        name,                       -- имя пользователя, указанное при регистрации
        country,                    -- Страна
        email,                      -- почта пользователя, указанная при регистрации
        account_language            -- язык ЛК пользователя, указанный на текущий момент
from  left_accounts
join  profile_info on left_accounts.account_id = profile_info.account_Id
where profile_info.type = 'standart'
      AND  not EXISTS (SELECT 1
                        FROM  affiliates
                        WHERE affiliates.child_id = profile_info.account_id
                        and partner_id is not null
                        )     
      and not EXISTS (select 1
                      from active_at_the_moment
                      where active_at_the_moment.account_id = left_accounts.account_Id  
                        )
      and account_language = 'ru'
      /*and not Exists  (select 1
                      from has_active_free_subscription
                      where has_active_free_subscription.account_id = left_accounts.account_Id 
                      )*/

/*
for test
with t  as (select account_id
    from `dwh-wazzup`.`dbt_swazzup`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates`
    where return_or_left_status_marketing in ('left')
    order by data_otvala desc)
    select wd.account_id,max(subscription_end)
    from t
    join `dwh-wazzup`.`dbt_swazzup`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates` wd on t.account_id = wd.account_id
    group by 1
    order by 2 desc*/