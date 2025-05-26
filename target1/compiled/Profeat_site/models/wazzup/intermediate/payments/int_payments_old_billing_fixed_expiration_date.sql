with billing_with_who_paid as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_old_billing_with_who_paid`
),

paidat_and_expires_at_from_eventlogs as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_who_paid_restore_missing_data__paidat_and_expiresat_deduplicated`
),

billing_data_with_fixed_dates as (
    select 
    billing_with_who_paid.account_id,
    billing_with_who_paid.start_date,
    billing_with_who_paid.guid,
    billing_with_who_paid.partner_account_id,
    billing_with_who_paid.start_at,
    paidat_and_expires_at_from_eventlogs.end_date as end_date
    from billing_with_who_paid 
    left join paidat_and_expires_at_from_eventlogs
    on billing_with_who_paid.start_date <= paidat_and_expires_at_from_eventlogs.start_date
    and billing_with_who_paid.start_date>=date_add(paidat_and_expires_at_from_eventlogs.start_date, interval -3 day)
    and billing_with_who_paid.guid=paidat_and_expires_at_from_eventlogs.subscription_id),

billing_data_with_fixed_dates_to_deduplicate as (
    select *, row_number() over (partition by guid, start_date order by end_date desc) rn from billing_data_with_fixed_dates
),

billing_data_with_fixed_dates_deduplicated as (
    select account_id,  -- ID аккаунта
    start_date,         -- Дата начала подписки
    guid,               -- guid платежа
    partner_account_id, -- ID аккаунта партнера
    start_at,           -- Дата и время начала подписки
    end_date            -- Дата окончания подписки
    from billing_data_with_fixed_dates_to_deduplicate
    where rn=1
)   -- Продвинутая таблица биллинга с исправленной датой окончания подписки. Последняя запись в 2022
select * from billing_data_with_fixed_dates_deduplicated