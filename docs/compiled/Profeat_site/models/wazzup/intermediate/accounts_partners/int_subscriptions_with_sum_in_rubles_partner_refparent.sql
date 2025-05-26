with payments_all as (
    select account_id, 
    start_date as paid_date, 
    currency,
    (case when partner_account_id is not null then 'partner_paid'
    else 'client_paid'
    end) as who_paid,
    sum(sum_in_rubles) as sum_in_rubles from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency`
    group by 1,2,3,4
),

partner_type_and_account_type as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated`
),

profile_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
),

payments as (select payments_all.account_id,
    payments_all.sum_in_rubles,
    payments_all.paid_date,
    payments_all.currency,
    partner_type_and_account_type.partner_id,
    partner_type_and_account_type.refparent_id,
    partner_type,
    account_type,
    partner_type_and_account_type.start_occured_at,
    partner_type_and_account_type.start_date,
    partner_register_date,
    who_paid
    from payments_all
    left join  partner_type_and_account_type 
    on payments_all.account_id=partner_type_and_account_type.account_id
    and payments_all.paid_date>=partner_type_and_account_type.start_date
    and payments_all.paid_date<=partner_type_and_account_type.end_date
    left join profile_info
    on profile_info.account_id=partner_type_and_account_type.partner_id
    where sum_in_rubles!=0),

payments_to_deduplicate as (

select *, row_number() over (partition by account_id, paid_date, currency,who_paid order by start_date desc) as rn  from payments
),

payments_deduplicated as (
  select account_id,        -- аккаунт дочки (кому принадлежит подписка)
    sum_in_rubles,          -- траты на подписку в рублях
    paid_date,              -- дата оплаты
    currency,               -- валюта
    partner_id,             -- аккаунт партнера
    refparent_id,           -- реферал аккаунта
    partner_type,           -- тип партнера
    account_type,           -- тип аккаунта
    start_occured_at,       -- Дата и время начала действия изменения
    start_date,             -- Дата начала действия изменения
    partner_register_date,  -- дата регистрации партнера
    who_paid,               -- кто оплачивал подписку
    date_trunc(paid_date, month) as paid_month     -- месяц оплаты
    from payments_to_deduplicate
    where rn=1
),

payments_with_overall_sum as (

select *, sum(sum_in_rubles) over (partition by partner_id) as sum_in_rubles_for_all_time from payments_deduplicated),


sum_distinct as (
    select distinct partner_id, sum_in_rubles_for_all_time from payments_with_overall_sum
    where sum_in_rubles!=0
),

partner_with_percentile_distinct as (

select partner_id, PERCENTILE_CONT(sum_in_rubles_for_all_time, 0.99) OVER() as percentile_99,   -- 99% перцентиль по тратам на подписку
PERCENTILE_CONT(sum_in_rubles_for_all_time, 0.50) OVER() as percentile_50,  -- 50% перцентиль по тратам на подписку
PERCENTILE_CONT(sum_in_rubles_for_all_time, 0.75) OVER() as percentile_75,  -- 75% перцентиль по тратам на подписку
PERCENTILE_CONT(sum_in_rubles_for_all_time, 0.25) OVER() as percentile_25   -- 25% перцентиль по тратам на подписку
 from sum_distinct),
 sum_in_rubles_with_percentile as (

select payments_with_overall_sum.*, percentile_25, percentile_50, percentile_75, percentile_99, 
(case when sum_in_rubles_for_all_time<=percentile_25 then 'percentile_25'
when sum_in_rubles_for_all_time<=percentile_50 then 'percentile_50'
when sum_in_rubles_for_all_time<=percentile_75 then 'percentile_75'
when sum_in_rubles_for_all_time<=percentile_99 then 'percentile_99'
else 'percentile 100'
 end ) as percentile from payments_with_overall_sum 
left join partner_with_percentile_distinct
on payments_with_overall_sum.partner_id=partner_with_percentile_distinct.partner_id)
    -- траты партнеров на подписку
select sum_in_rubles_with_percentile.* 
from sum_in_rubles_with_percentile
where not exists (
    select account_id
    from profile_info
    where is_employee
    and profile_info.account_id = sum_in_rubles_with_percentile.account_id
)
and not exists (
        select account_id
    from profile_info
    where is_employee
    and profile_info.account_id = sum_in_rubles_with_percentile.partner_id
)
and not exists (
    select account_id
    from profile_info
    where is_employee
    and profile_info.account_id = sum_in_rubles_with_percentile.refparent_id
)