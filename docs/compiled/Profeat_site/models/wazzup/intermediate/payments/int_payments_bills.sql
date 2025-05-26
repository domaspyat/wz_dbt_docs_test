-- Продвинутая таблица счетов
select account_id, --case when id = 138647 then 60391971 else account_id end as account_id, --некорректная оплата в рамках задачи 1424214
       paid_date,         --case when id = 138647 then date('2024-09-30') else paid_date end  paid_date, 
    currency,           -- Валюта
    sum_in_rubles,      -- Сумма оплаты в рублях
    original_sum,       -- Сумма оплаты
    guid,               -- Идентификатор счета. Генерируется Postgress при создании записи в формате string
    subscription_id,    -- ID подписки
    updated_at,         -- Дата и время обновления счета
    completed_at,       -- Дата и время оплаты счета
    status,             -- Состояние счета
    id,                 -- Идентификатор счета. Генерируется Postgress при создании записи в формате int
    coalesce(cast(paid_in_wazzup_at as date), cast(completed_at as date), cast(updated_at as date),(case when paid_date='1970-01-01' then cast(null as date) else paid_date end)) as billing_date_subscription_start,   -- Дата начала подписки по биллингу
    coalesce(cast(paid_in_wazzup_at as date),cast(updated_at as date)) as paid_in_wazzup_date   -- Дата поступления денег в Wazzup
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_bills`
    where status in ('paid','paidInvalid') and not (account_id=96943190 and status='paidInvalid') 
    and json_value(details,'$.paymentInvalidationReason') is distinct from 'tech-partner-postpay' 
    and  id!=101910
    and  id != 138647