with    -- Таблица платежей с суммой != 0
    payments as (
        select *
        from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_old_and_new_data_union`
        where sum != 0
    ),

    payments_with_currency as (

        select      -- Таблица платежей с валютами и датой
            sum,                            -- Сумма платежа
            payments.currency,              -- Валюта
            start_date,                     -- Дата начала
            (
                case
                    when payments.currency = 'RUR'
                    then sum
                    when rur is not null
                    then sum * rur
                    when payments.currency = 'EUR' and rur is null
                    then sum * 85
                    when payments.currency = 'USD' and rur is null
                    then sum * 75
                    when payments.currency = 'KZT' and rur is null
                    then sum * 0.24
                end
            ) as sum_in_rubles, -- Сумма в рублях с фиксированным курсом
            coalesce(
                (
                    case
                        when payments.currency = 'RUR'
                        then wapi_transactions
                        when rur is not null
                        then wapi_transactions * rur
                        when payments.currency = 'EUR' and rur is null
                        then wapi_transactions * 85
                        when payments.currency = 'USD' and rur is null
                        then wapi_transactions * 75
                        when payments.currency = 'KZT' and rur is null
                        then wapi_transactions * 0.24
                    end
                ),
                0
            ) as wapi_transactions_in_rubles,   -- Сумма транзакции WABA в рублях с фиксированным курсом
            sum as original_sum,                -- Оригинальная сумма оплаты
            coalesce(wapi_transactions, 0) as wapi_original_sum,    -- Оригинальная сумма транзакции WABA
            (
                case
                    when payments.currency = 'USD'
                    then sum
                    when usd is not null
                    then sum / usd
                    when payments.currency = 'EUR' and usd is null
                    then sum / 1.12
                    when payments.currency = 'USD' and usd is null
                    then sum * 75
                    when payments.currency = 'KZT' and usd is null
                    then sum * 0.24
                end
            ) as sum_in_usd,        -- Сумма оплаты в долларах с фиксированным курсом
            (
                case
                    when payments.currency = 'USD'
                    then wapi_transactions
                    when usd is not null
                    then wapi_transactions / usd
                    when payments.currency = 'EUR' and usd is null
                    then wapi_transactions / 1.12
                    when payments.currency = 'USD' and usd is null
                    then wapi_transactions * 75
                    when payments.currency = 'KZT' and usd is null
                    then wapi_transactions * 0.24
                end
            ) as wapi_sum_in_usd,   -- Сумма транзакции WABA в долларах с фиксированным курсом

            guid,                   -- guid из subscriptionUpdates
            partner_account_id,     -- ID аккаунта партнера
            subscription_type,      -- Тип подписки: viber, telegram, whatsapp, waba, avito, vk, tgapi, instagram
            account_id              -- ID аккаунта
        from payments
        left join
            `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted
            on exchange_rates_unpivoted._ibk = payments.start_date
            and exchange_rates_unpivoted.currency = payments.currency
    )

select payments_with_currency.*
from payments_with_currency
left join
    `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts
    on accounts.account_id = payments_with_currency.account_id
where accounts.type not in ('employee', 'partner-demo')
-- Таблица платежей с суммой оплаты в оригинальной валюте, рублях и долларах с фиксированным курсом