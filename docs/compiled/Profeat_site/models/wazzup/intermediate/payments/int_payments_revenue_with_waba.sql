with revenue as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_with_account_and_partner_type`
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
    where account_type not in ('employee','partner-demo','child-postpay') and partner_type is distinct from 'employee'
),
waba_revenue_postpay_grouped_by_paid_date_and_partner_id as (
    select account_id,
    paid_date,
    currency,
    sum(waba_sum_in_rubles) as waba_sum_in_rubles
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_postpay_waba_revenue_gs`
    group by 1,2,3
),

waba_revenue_postpay as (
    select cast(account_id as integer) as account_id,
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
    account_Id,             -- ID аккаунта
    paid_date,              -- Дата оплаты
    partner_id,             -- ID аккаунта
    refparent_id,           -- ID аккаунта реф. партнера
    currency,               -- Валюта
    paid_month,             -- Месяц оплаты
    account_type,           -- Тип аккаунта
    partner_type,           -- Тип аккаунта партнера
    data_source,            -- Источник оплаты
    sum_in_rubles,          -- Сумма оплаты в рублях
    sum_in_USD,             -- Сумма оплаты в долларах
    original_sum,           -- Сумма оплаты
    partner_register_date,  -- Дата регистрации партнера
    (case when rn!=1 then 0 else waba_sum_in_rubles end) waba_sum_in_rubles,    -- Сумма оплаты баланса WABA в рублях
    (case when rn!=1 then 0 else waba_original_sum end) waba_original_sum,      -- Сумма оплаты баланса WABA
    (case when rn!=1 then 0 else waba_sum_in_USD end) waba_sum_in_USD           -- Сумма оплаты баланса WABA в долларах
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
    when partner_type='tech-partner-postpay' then 'tech-partner-postpay'
    else 'unknown'
    end
    ) as segment_type -- Тип сегмента клиента
     from revenue_and_waba_joined_fixed),


revenue_with_segments_aggregated as (
    select *, (case when segment_type in ('standart','referal','of-partner-client') then 'final_client'
    when segment_type='of-partner' then 'of-partner'
    when segment_type in ('tech-partner','tech-partner-client','tech-partner-postpay') then 'tech-partner'
    else 'unknown'
    end
    ) as segments_aggregated    -- Сегмент клиента после группировки 
    from revenue_with_segments
),
profile_info as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
)

select revenue_with_segments_aggregated.*,  -- Таблица дохода с клиентов, учитывая WABA
        russian_country_name,   -- Страна клиента на русском языке
        region_international    -- Регион клиента
from revenue_with_segments_aggregated
left join  profile_info
on revenue_with_segments_aggregated.account_id=profile_info.account_id 
where not exists (
    select account_id
    from profile_info
    where profile_info.account_id = revenue_with_segments_aggregated.account_id
    and is_employee
)