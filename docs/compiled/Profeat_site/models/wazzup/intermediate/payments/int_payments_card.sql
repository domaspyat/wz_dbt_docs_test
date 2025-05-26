with payments_card as (select
    account_id,                 -- ID аккаунта
    paid_date,                  -- Дата перехода платежа в state = completed
    paid_at,                    -- Дата и время перехода платежа в state = completed
    payments.currency,          -- Валюта
    (case
        when payments.currency = 'RUR'  then sum
        when RUR is not null then  (sum-vat) * RUR
        when payments.currency = 'EUR'  and RUR is null then  (sum-vat) * 85 
        when payments.currency = 'USD'  and RUR is null then  (sum-vat) * 75
        when payments.currency = 'KZT' and RUR is null then  (sum-vat) * 0.24
    end) as sum_in_rubles,      -- Сумма оплаты в рублях
    (sum-vat) as original_sum,  -- Сумма оплаты
    (case
        when payments.currency = 'USD'  then sum-vat
        when USD is not null then  (sum-vat) / USD
        when payments.currency = 'EUR'  and USD is null then  (sum-vat) /1.12
        when payments.currency = 'USD'  and USD is null then  (sum-vat) * 75
        when payments.currency = 'KZT' and USD is null then  (sum-vat) * 0.24
        end 
    ) as sum_in_USD,            -- Сумма оплаты в долларах
    subscription_update_id,     -- ID изменения. Соответствует guid из subscriptionUpdates
    payment_provider,           -- Обозначает источник платежа: yandexkassa - Яндекс Касса, tinkoff - Тинькофф, stripe - Stripe, cashless - безнал, в этом месте не актуально, intellect_money/intellectMoney - устарело в связи с уходом от IntellectMoney, partner - Оплата с партнерского счета. При этом sum = 0, setPromisedPayment - Обещанный платеж. При этом sum = 0
    is_spb_payment,             -- Является ли платежом через систему быстрых платежей (СПБ)
    guid as guid,               -- Идентификатор оплаты.Генерируется Postgress при создании записи
    partner_account_id,         -- ID аккаунта партнера, если платил партнер
    subscription_id,            -- ID подписки
    details,                    -- JSON с различными полями, деталями подписки. Подробнее в wazzup_staging_payments.yml
    active_until,               -- Дата и время окончания обещанного платежа по тайм зоне клиента
    payments_start_date,        -- Дата и время активации обещанного платежа по тайм зоне клиента
    promised_payment_type,      -- Тип обещанного платежа. Возможные значения: renewal, pay, addQuantity, raiseTariff
    promised_payment_start,     -- Дата и время начала обещанного платежа
    promised_payment_end_date   -- Дата и время окончания обещанного платежа
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_card`  payments
    left join `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted 
    on exchange_rates_unpivoted._ibk = payments.paid_date
    and exchange_rates_unpivoted.currency = payments.currency
    where state = 'completed'
  ) -- Продвинутая таблица оплат
select * from payments_card