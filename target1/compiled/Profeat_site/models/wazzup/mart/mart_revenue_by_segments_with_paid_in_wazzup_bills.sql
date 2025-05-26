with revenue as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_with_account_and_partner_type_with_bills_date`
    where account_type not in ('employee','partner-demo','child-postpay') and partner_type is distinct from 'employee'
),

waba_revenue as (
    select account_id, 
    paid_at_billing_date as paid_date,
    paid_month,
    currency,
    partner_id,
    refparent_id, 
    account_type,
    partner_type,
    partner_register_date,
    sum_in_rubles as waba_sum_in_rubles,
    original_sum as waba_original_sum,
    wapi_sum_in_USD as waba_sum_in_USD
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_waba_with_sum_and_currency`
    where account_type not in ('employee','partner-demo') and partner_type is distinct from 'employee'
),
waba_revenue_postpay_grouped_by_paid_date_and_partner_id as (
    select partner_id as account_id,
    paid_date,
    currency,
    sum(amount) as waba_sum_in_rubles
    from `dwh-wazzup`.`partners_info`.`postpay_waba_revenue`
    group by 1,2,3
),

waba_revenue_postpay as (
    select cast(account_id as integer),
    paid_date,
    date_trunc(paid_date,month) as paid_month,
    currency,
    cast(null as integer) as partner_id,
    cast(null as integer) as refparent_id,
    'tech-partner-postpay' as account_type,
    cast(null as string) as partner_type,
    cast(null as datetime) as partner_register_date,
    waba_sum_in_rubles as waba_sum_in_rubles,
    waba_sum_in_rubles as waba_original_sum,
    cast(null as float64) as waba_sum_in_USD
    from waba_revenue_postpay_grouped_by_paid_date_and_partner_id   
),
waba_all_revenue as (
    select * from waba_revenue
    UNION ALL 
    select * from waba_revenue_postpay
),

    revenue_and_waba_joined as (
    select 
    coalesce(revenue.account_id, waba_revenue.account_id) as account_id,
    coalesce(revenue.paid_date, waba_revenue.paid_date) as paid_date,
    coalesce(revenue.partner_id, waba_revenue.partner_id) as partner_id,
    coalesce(revenue.refparent_id, waba_revenue.refparent_id) as refparent_id,
    coalesce(revenue.currency, waba_revenue.currency) as currency,
    coalesce(revenue.paid_month, waba_revenue.paid_month) as paid_month,
    coalesce(revenue.account_type, waba_revenue.account_type) as account_type,
    coalesce(revenue.partner_type, waba_revenue.partner_type) as partner_type,
    coalesce(revenue.partner_register_date, waba_revenue.partner_register_date) as partner_register_date,
    data_source,
    ifnull(sum_in_rubles,0) as sum_in_rubles,
    ifnull(sum_in_USD,0) as sum_in_USD,
    ifnull(original_sum,0) as original_sum,
    ifnull(waba_sum_in_rubles,0) as waba_sum_in_rubles,
    ifnull(waba_original_sum,0) as waba_original_sum,
    ifnull(waba_sum_in_USD,0) as waba_sum_in_USD,
    from revenue full outer join waba_all_revenue waba_revenue 
    on revenue.account_id=waba_revenue.account_id
    and revenue.paid_date=waba_revenue.paid_date
    and revenue.partner_id=waba_revenue.partner_id
    and revenue.refparent_id=waba_revenue.refparent_id
    and revenue.currency=waba_revenue.currency
    and revenue.paid_month=waba_revenue.paid_month
    and revenue.account_type=waba_revenue.account_type
    and revenue.partner_type=waba_revenue.partner_type
    and revenue.partner_register_date=waba_revenue.partner_register_date
    ),

revenue_and_waba_joined_to_deduplicate_waba as (
    select *, rank() over (partition by account_id,paid_date order by data_source) rn from revenue_and_waba_joined),

revenue_and_waba_joined_fixed as (

    select 
    account_Id,             -- аккаунт, который проводил оплату
    paid_date,              -- дата оплаты. Даты для безнала соответствуют датам оплаты подписки в wazzup
    partner_id,             -- ID аккаунта партнера
    refparent_id,           -- ID аккаунта реф. партнера
    currency,               -- валюта
    paid_month,             -- месяц оплаты
    account_type,           -- тип аккаунта
    partner_type,           -- тип партнера (оф. партнер, тех. партнер, тех. партнер постоплата)
    data_source,            -- Источник оплаты
    sum_in_rubles,          -- сумма в рублях. Конвертация происходит на день оплаты
    sum_in_USD,             -- сумма в долларах
    original_sum,           -- сумма в валюте
    partner_register_date,  -- дата регистрации партнера
    (case when rn!=1 then 0 else waba_sum_in_rubles end) waba_sum_in_rubles,    -- траты на вабу в рублях
    (case when rn!=1 then 0 else waba_original_sum end) waba_original_sum,      -- траты на вабу в валюте
    (case when rn!=1 then 0 else waba_sum_in_USD end) waba_sum_in_USD           -- траты на вабу в долларах
    from revenue_and_waba_joined_to_deduplicate_waba),

revenue_with_segments as (
    select *, (case when account_type='partner' then 'of-partner'
    when account_type='tech-partner' then 'tech-partner'
    when account_type='tech-partner-postpay' then 'tech-partner-postpay'
    when account_type='standart' and partner_id is null and refparent_id is null then 'standart'
    when account_type='standart' and partner_type is null and partner_id is not null and paid_date<partner_register_date then 'standart'
    when account_type='standart' and partner_id is null and refparent_id is not null then 'referal'
    when account_type='standart' and partner_type in ('partner','standart') then 'of-partner-client'
    when account_type='standart' and partner_type='tech-partner' then 'tech-partner-client'
    else 'unknown'
    end
    ) as segment_type       -- тип сегмента: standart - клиент без партнера и реферала, of-partner - оф. партнер, referal - реферал, tech-partner-postpay - постоплатник, tech-partner - тех. партнер предоплатник, tech-partner-client - дочка тех. партнера,  of-partner-client - дочка оф. партнера
    from revenue_and_waba_joined_fixed),


revenue_with_segments_aggregated as (
    select *, (case when segment_type in ('standart','referal','of-partner-client') then 'final_client'
    when segment_type='of-partner' then 'of-partner'
    when segment_type in ('tech-partner','tech-partner-client','tech-partner-postpay') then 'tech-partner'
    else 'unknown'
    end
    ) as segments_aggregated -- агрегированные сегменты: оф. партнер, тех. партнер и конечные клиенты
    from revenue_with_segments
)
    -- Выручка по дням. Даты для безнала соответствуют датам оплаты подписки в wazzup
select revenue_with_segments_aggregated.*, russian_country_name     -- Название страны на русском языке
from revenue_with_segments_aggregated
left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
on revenue_with_segments_aggregated.account_id=profile_info.account_id 
where is_employee is distinct from True