with accounts as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
),

eventLogs as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_eventLogs`
)

    -- Таблица с минимальной датой автопродления по аккаунту
select  accounts.account_id,                                -- ID аккаунта
        cast(min(paid_at) as date) as min_autorenewal_date  -- Минимальная дата автопродления
from eventLogs
    join accounts  
        on eventLogs.account_id=accounts.account_id
where log_Type='billingPackages' and autorenewal
    and eventLogs._ibk>='2022-04-01'
group by 1