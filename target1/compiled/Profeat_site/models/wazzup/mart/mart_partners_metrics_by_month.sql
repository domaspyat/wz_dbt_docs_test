with clients_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_is_client_active_by_month`
),
payments_by_month as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_revenue_by_month`
),


profile_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
),

country as (
    select * from `dwh-wazzup`.`analytics_tech`.`country`
),

billings as (SELECT  account_id,
                     max(case when action='renewal' and billing_packages.paid_at is not null then True end) as is_renewal,
                     max(case when billing_packages.paid_at is not null and billing_packages.period=12 and cast(billing_packages.created_at as date)<=date_add(current_date, interval 12 month) then True end) as subscription_12,
                     max(case when billing_packages.paid_at is not null and billing_packages.period=6 and cast(billing_packages.created_at as date)<=date_add(current_date, interval 6 month) then True end) as subscription_6,
                     max(case when billing_packages.paid_at is not null and billing_packages.period=1  and cast(billing_packages.created_at as date)<=date_add(current_date, interval 6 month) then True end) as subscription_1
FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates`  substriprion_updates

inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billing_packages 
on substriprion_updates.subscription_id=billing_packages.guid
where substriprion_updates.state='activated'
group by 1),

subscription_sum as (
    select 	account_id, 
    paid_month, 
    sum(sum_in_rubles) as sum_in_rubles_subscriptions
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_in_rubles_partner_refparent`
    group by 1,2
),

partners_by_month as (
select month, 
coalesce(payments_by_month.partner_id,clients_info.partner_id) as partner_id, 
partner_register_date,
region_type, 
profile_info.russian_country_name as russianName, 
city, 
region, 
profile_info.type,
profile_info.currency, 
is_employee,
active_account_id, 
all_account_id,
is_renewal,
subscription_12,
subscription_6,
subscription_1 ,
 sum_in_rubles_subscriptions,
 sum_in_rubles,
sum_in_rubles_partner_paid, 
from clients_info left join payments_by_month
 on clients_info.month=payments_by_month.paid_month 
 and clients_info.partner_id=payments_by_month.partner_id
left join  profile_info 
on clients_info.partner_id=profile_info.account_id
left join billings on billings.account_id=clients_info.active_account_id
left join subscription_sum on subscription_sum.account_id=clients_info.active_account_id and subscription_sum.paid_month=clients_info.month
),

partners_without_clients as (
select month.month,                                     -- Отчетный месяц
profile_info.account_id as partner_id,                  -- Account id партнера, для которого собираются метрики
partner_register_date,                                  -- Дата получения партнерки
region_type,                                            -- регион (СНГ, НЕ-СНГ, Неизвестно)
russian_country_name as russianName,                    -- Название страны на русском языке
city,                                                   -- город
region,                                                 -- область
type,                                                   -- тип аккаунта
currency,                                               -- валюта партнера
profile_info.is_employee,                               -- это аккаунт сотрудника?
cast(null as int64) as active_account_id,               -- заполняется, если дочка была активная в этом месяце. иначе null
cast(null as int64) as all_account_id,                  -- заполняется, если дочка была привязана к партнеру в этом месяце
cast(null as bool) as is_renewal,                       -- True, если подписка клиента была продлена когда-либо
cast(null as bool) as subscription_12,                  -- True, если у клиента была когда-либо подписка на 12 месяцев
cast(null as bool) as subscription_6,                   -- True, если у клиента была когда-либо подписка на 6 месяцев
cast(null as bool) as subscription_1 ,                  -- True, если у клиента была когда-либо подписка на 1 месяц
 cast(null as float64) as sum_in_rubles_subscriptions   -- траты на подписку клиента в этом месяце
 from profile_info
left join dwh-wazzup.analytics_tech.months month
on month.month >= date_trunc(profile_info.partner_register_date,month)

where profile_info.type in ('partner','tech-partner') and not exists (select account_id from partners_by_month 
where partners_by_month.partner_id=profile_info.account_id)),

partners_without_clients_with_payments as (
select partners_without_clients.*, 
sum_in_rubles_partner_paid,             -- выручка от партнерского аккаунта в этом месяце
sum_in_rubles                           -- выручка от партнерского аккаунта + аккаунтов клиентов в этом месяце
from partners_without_clients 
left join payments_by_month
on partners_without_clients.partner_id=payments_by_month.partner_id
and partners_without_clients.month=payments_by_month.paid_month),

partners_and_clients as (
select * 
from partners_by_month
union all
select * 
from partners_without_clients_with_payments)
    -- Партнерские метрики по месяцам
select *except(is_employee) 
from partners_and_clients
where is_employee is false

and not exists (
        select account_id
    from profile_info
    where profile_info.account_id = partners_and_clients.all_account_id
    and is_employee
)