with metrics_info as (select partner_id,
'register' as event,
cast(null as string) as currency,
cast(null as string) as segments_aggregated,
min(date_trunc(partner_register_date,MONTH)) as month,
sum(cast(null as float64)) as sum,
sum(cast(null as float64)) as original_sum,
sum(cast(null as float64)) as waba_sum_in_rubles,
sum(cast(null as float64)) as waba_original_sum
from `dwh-wazzup`.`dbt_nbespalov`.`mart_partners_metrics_by_month`
group by 1,2,3,4

union all 

select account_id as partner_id,
'payment' as event,
 currency as currency,
 cast(null as string) as segments_aggregated,
min(paid_month) as month, 
sum(sum_in_rubles) as sum,
sum(cast(null as float64)) as original_sum,
sum(cast(null as float64)) as waba_sum_in_rubles,
sum(cast(null as float64)) as waba_original_sum
from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_with_waba`
where segment_type in ('of-partner','tech-partner')
group by 1,2,3,4


UNION ALL 

select partner_id,
'partner_50' as event,
cast(null as string) as currency,
cast(null as string) as segments_aggregated,
date_trunc(occured_date,MONTH) as month,
null as sum, 
null as original_sum,
cast(null as float64) as waba_sum_in_rubles,
cast(null as float64) as waba_original_sum
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_fifty_discount_by_month_and_account`

union all

select partner_id as partner_id,
'tech_partner_payment' as event,
 cast(null as string) as currency,
 cast(null as string) as segments_aggregated,
min(month) as month, 
sum(cast(null as float64)) as sum,
sum(cast(null as float64)) as original_sum,
sum(cast(null as float64)) as waba_sum_in_rubles,
sum(cast(null as float64)) as waba_original_sum
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_is_client_active_by_month`
where active_account_id is not null
group by 1,2,3,4

union all 
select partner_id,
'discount' as event,
currency,
cast(null as string) as segments_aggregated,
paid_month as month,
discount_sum_in_rubles as sum, 
discount_sum_original as original_sum ,
cast(null as float64) as waba_sum_in_rubles,
cast(null as float64)as waba_original_sum
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_discount_by_month_and_account`

UNION ALL 

select account_id as partner_id,
'reward' as event,
currency,
cast(null as string) as segments_aggregated,
paid_month as month,
sum_in_rubles as sum, 
original_sum as original_sum ,
cast(null as float64) as waba_sum_in_rubles,
cast(null as float64) as waba_original_sum
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_reward_by_month_and_account`

UNION ALL

select (case when account_type in ('partner','tech-partner') then account_id
when partner_type='tech-partner' then partner_id
end
) as partner_id,
'revenue' as event,
 currency,
 (case when segments_aggregated='of-partner' then 'partner'
when segments_aggregated='tech-partner' then 'tech-partner'
end ) as segments_aggregated,
paid_month as month,

sum(sum_in_rubles) as sum,
sum(original_sum) as original_sum,
sum(waba_sum_in_rubles) as waba_sum_in_rubles,
sum(waba_original_sum) as waba_original_sum
from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_with_waba`
where segment_type in ('of-partner','tech-partner') or partner_type='tech-partner'
group by 1,2,3,4,5

),top_100 as (
select 
      account_id,
      current_quarter
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_top_100_ru_of_partners`
) 
    -- Агрегированные метрики партнерки
select metrics_info.partner_id, -- Аккаунт партнера, для которого собираются метрики
metrics_info.event,             -- reward - реферальные выплаты партнеру за подписки, payment - первое пополнение ЛК партнером (когда он был в статусе партнера), revenue - выручка при пополнении ЛК, discount - скидка при оплате подписки, register - дата регистрации партнера, partner_50 - первая оплата подписки при скидке 50%, tech_partner_payment - первая оплата подписки тех. партнером либо его дочкой
metrics_info.currency,          -- валюта для оплат (revenue, discount)
metrics_info.month,             -- месяц
metrics_info.sum,               -- сумма для reward
metrics_info.original_sum,      -- сумма в валюте для revenue и discount
metrics_info.waba_sum_in_rubles,-- сумма пополнения баланса вабы в рублях
metrics_info.waba_original_sum, -- сумма пополнения баланса вабы в валюте
profile_info.region_type,       -- регион (СНГ, НЕ-СНГ, Неизвестно)
profile_info.country,           -- Страна
coalesce(segments_aggregated, profile_info.type) as  account_type,  -- тип аккаунта партнера (оф. партнер, тех. партнер)
profile_info.currency as partner_currency,                          -- валюта ЛК партнера
(case when top_100.account_id is not null then True else False end) as is_top_100   -- есть ли этот партнер в топ-100
 from metrics_info
left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info 
            on metrics_info.partner_id=profile_info.account_id 
left join  top_100
        on top_100.account_id=metrics_info.partner_id
            and date_trunc(metrics_info.month,quarter) = top_100.current_quarter
where is_employee is false