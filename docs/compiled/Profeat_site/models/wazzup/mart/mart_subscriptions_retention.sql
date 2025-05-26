with retention_based_on_subscriptions as(
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_creating_intervals_based_on_first_subscription_period_with_seven_or_more_days`
    where cnt >= 7
    and account_type != 'employee'
),
retention_based_on_registrations as(
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_creating_intervals_based_on_first_registration_period_with_seven_or_more_days`
    where cnt >= 7 
    and account_type != 'employee'
),
union_operation as (
select *,'subscriptions' as type
from retention_based_on_subscriptions
union all
select *,'registrations' as type
from retention_based_on_registrations)
select *    -- Таблица используется для когоротного анализа по подпискам.Она нужна, чтобы отслеживать активность подписки пользователя В данном отчете не важно, кто именно внес оплату за подписку. Важно то, кому принадлежала подписка!
from union_operation
where subscription_period is null or subscription_period not in (3,24)