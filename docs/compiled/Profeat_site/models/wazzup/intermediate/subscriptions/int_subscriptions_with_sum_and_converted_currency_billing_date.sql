with payments as (          -- Таблица оплат подписок с суммой неравной нулю
    select *  from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_subscripton_with_sum`
    where sum!=0
),

payments_with_currency as ( -- Таблица оплат с валютами

select                      -- Детальная таблица оплат
    sum,                                                                                -- Сумма оплаты
    payments.currency,                                                                  -- Валюта оплаты: RUR, USD, EUR, KZT
    paid_at_billing,                                                                    -- Дата и время оплаты (04.03.2025)
    paid_at_billing_date,                                                               -- Дата оплаты
    paid_at_billing_completed_at,                                                       -- Дата и время завершения оплаты (04.03.2025)
    paid_at_billing_completed_date,                                                     -- Дата завершения оплаты
    period_new,                                                                         -- Период оплаты: 1, 6, 12 месяцев
    (case
        when payments.currency = 'RUR'  then sum
        when RUR is not null then  sum * RUR
        when payments.currency = 'EUR'  and RUR is null then  sum * 85 
        when payments.currency = 'USD'  and RUR is null then  sum * 75
        when payments.currency = 'KZT' and RUR is null then  sum * 0.24
    end) as sum_in_rubles,                                                              -- Конвертация суммы из других валют в рубли с фикс курсом
    coalesce((case
        when payments.currency = 'RUR'  then wapi_transactions
        when RUR is not null then  wapi_transactions * RUR
        when payments.currency = 'EUR'  and RUR is null then  wapi_transactions * 85 
        when payments.currency = 'USD'  and RUR is null then  wapi_transactions * 75
        when payments.currency = 'KZT' and RUR is null then  wapi_transactions * 0.24
    end),0) as wapi_transactions_in_rubles,                                             -- Конвертация суммы из других валют в рубли для баланса WABA с фикс курсом
    sum as original_sum,                                                                -- Сумма оплаты в исходной валюте
    coalesce(wapi_transactions,0) as wapi_original_sum,                                 -- Сумма оплаты баланса WABA в исходной валюте
    (case
        when payments.currency = 'USD'  then sum
        when USD is not null then  sum / USD
        when payments.currency = 'EUR'  and USD is null then  sum /1.12
        when payments.currency = 'USD'  and USD is null then  sum * 75
        when payments.currency = 'KZT' and USD is null then sum * 0.24
        end
    ) as sum_in_USD ,                                                                   -- Конвертация суммы из других валют в доллары с фикс курсом
    (case
        when payments.currency = 'USD'  then wapi_transactions
        when USD is not null then  wapi_transactions / USD
        when payments.currency = 'EUR'  and USD is null then  wapi_transactions /1.12
        when payments.currency = 'USD'  and USD is null then  wapi_transactions * 75
        when payments.currency = 'KZT' and USD is null then wapi_transactions * 0.24
        end
    ) as wapi_sum_in_USD ,                                                              -- Конвертация суммы из других валют в доллары для баланса WABA с фикс курсом
    guid,                                                                               -- guid оплаты
    partner_account_id,                                                                 -- ID аккаунта партнера, совершившего оплату
    subscription_type,                                                                  -- Тип подписки
    balance_to_withdraw,                                                                -- В случае частичной оплаты кол-во бонусов клиента используемых для оплаты подписки    
    partner_discount,                                                                   -- Скидка партнера - 35% или 50%
    subscription_id,                                                                    -- ID подписки
    account_id,                                                                         -- ID аккаунта
    activation_reason_id,                                                               -- id/guid записи, на основании которого было применоно(оплачено) изменение. Соответствует полю guid из таблицы payments для activationObject = payment или partnerBalance. Соответствует полю id из таблицы bills для activationObject = invoice
    action                                                                              -- Действие с подписок: renewal, addQuantity, pay, raiseTariff, balanceTopup, templateMessages
    from payments
    left join `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted 
    on exchange_rates_unpivoted._ibk = payments.paid_at_billing_date
    and exchange_rates_unpivoted.currency = payments.currency )

select * from payments_with_currency