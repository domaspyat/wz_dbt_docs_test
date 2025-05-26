with transaction_currency as (
   select subscription_id, 
  cast(date_trunc(date_at, month) as  date) as paid_month, 
  currency,
  row_number() over (partition by subscription_id, date_trunc(date_at, month) order by date_at, id desc) rn ,
  min(cast(date_trunc(date_at, month) as  date)) over (partition by subscription_id) as first_month
  from dwh-wazzup.dbt_prod.stg_channels_waba_transactions
),

calendar_subscription_id as (
  select distinct subscription_id, month, first_month from transaction_currency
  cross join dwh-wazzup.analytics_tech.months
  where month>=first_month and month<=date_trunc(current_date, month)
),

transaction_currency_deduplicated as (
  select * from transaction_currency
  where rn=1
),

calendar_with_currency_data as (

select calendar_subscription_id.*, currency from  calendar_subscription_id 
left join transaction_currency_deduplicated on transaction_currency_deduplicated.paid_month=calendar_subscription_id.month and transaction_currency_deduplicated.subscription_id=calendar_subscription_id.subscription_id),
calendar_to_fillna as (
 select *,
               sum(case when currency is not null then 1 else 0 end) over (partition by subscription_id
          order by month asc) as r_close
          from calendar_with_currency_data),

calendar_filled_na as (

select * , 
          first_value(currency) over (partition by subscription_id, r_close order by month asc rows between unbounded preceding and unbounded following) as currency_filled
          from calendar_to_fillna
          )
    -- Таблица c валютой подписки WABA по месяцам
select subscription_id,         -- ID подписки
currency_filled as currency,    -- Валюта
month   -- Месяц
 from calendar_filled_na
 where subscription_id is not null