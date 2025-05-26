with billing_data as (
select  account_id,
        guid,
        paid_at, 
        object,
        payment_method,
        start_at,
        end_at
        from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_old_billing` 
        ),

billing_data_with_nulls as (
select *, 
sum(case when payment_method is not null then 1 end) over (partition by account_id order by paid_at) as r_close

from billing_data),

billing_data_with_fill_null as 

(select account_id,
        guid,
        object,
        paid_at, 
        start_at,
        end_at,
        first_value(payment_method) over (partition by account_id,r_close order by paid_at asc) as partner_account_id 
        from  billing_data_with_nulls),
                      
billing_data_cleared as (
    select account_id,          -- ID аккаунта
        guid,                   -- guid платежа
        paid_at as start_at,    -- Дата и время начала подписки
        end_at,                 -- Дата и время окончания подписки
        cast(paid_at as date) as start_date,-- Дата начала подписки
        cast(end_at as date) as end_date,   -- Дата окончания подписки
        cast(partner_account_id as int) partner_account_id, -- ID аккаунта партнера
        paid_at                 -- Дата и время оплаты
        from billing_data_with_fill_null
        where object='package' and paid_at is not null
)
    -- Продвинутая таблица биллинга с информацией о плательщике. Последняя запись в 2022
select * from billing_data_cleared