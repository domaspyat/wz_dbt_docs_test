with bills_pay as (
    select account_id,
            paid_in_wazzup_date as paid_date, 
            currency,
            sum_in_rubles,
            original_sum,
            null as sum_in_USD,
             null as partner_account_id, 
            'bills_pay' as data_source
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_bills`
),
card_pay as (
    select  account_id,
            paid_date,
            currency,
            sum_in_rubles,
            original_sum,
            sum_in_USD,
            partner_account_id,
            (case when is_spb_payment then 'card_pay_spb_payment'
            when not is_spb_payment then 'card_pay_tinkof_not_sbp'
            when payment_provider is not null then CONCAT('card_pay_', payment_provider)
            else 'card_pay'
            end)
             as data_source 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_card`
    where original_sum!=0
),

bank_pay as (
    select account_id,
            paid_date,
            currency,
            sum_in_rubles,
            original_sum,
            null as sum_in_USD,
            null as partner_account_id,
            'bank_pay' as data_source 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_bank`

    UNION ALL
    select account_id,
            paid_date,
            currency,
            sum_in_rubles,
            original_sum,
            null as sum_in_USD,
            null as partner_account_id,
            'bank_pay' as data_source 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_old_billing`
    where object = 'payment'
    and method in ('bank', 'paypal')
),

postpay_bills as (
    select cast(account_id as integer) as account_id,
            paid_date,
            currency,
            sum_in_rubles,
            original_sum,
            null as sum_in_USD,
             null as partner_account_id,
            'post_pay' as data_source 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_postpay_revenue_bills`

),

all_revenue as 
(select account_id,                             -- ID аккаунта
        paid_date,                              -- Дата оплаты
        currency,                               -- Валюта
        data_source,                            -- Источник оплаты
        partner_account_id,                     -- ID аккаунта партнера, если он платил
        sum(sum_in_rubles) as sum_in_rubles,    -- Сумма оплаты в рублях
        sum(sum_in_USD) as sum_in_USD,          -- Сумма оплаты в долларах
        sum(original_sum) as original_sum       -- Оригинальная сумма оплаты
from (
    select * from bills_pay
    UNION ALL
    select * from card_pay
    UNION ALL 
    select * from bank_pay
    UNION ALL 
    select * from postpay_bills
    ) rev
group by 1,2,3,4,5)
    -- Таблица платежей, дошедших до Wazzup с валютами и источником оплаты 
select * from all_revenue