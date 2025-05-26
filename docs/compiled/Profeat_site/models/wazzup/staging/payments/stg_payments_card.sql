with eventlogs as (                   -- Таблица оплат
  select distinct TIMESTAMP_TRUNC(cast(promised_payment_start as timestamp), hour) as promised_payment_start,
  TIMESTAMP_TRUNC(cast(promised_payment_start_date as timestamp), hour) as promised_payment_start_date ,
  subject_id,
  promised_payment_type,
  TIMESTAMP_TRUNC(promised_payment_end_date, hour) as promised_payment_end_date
   from  `dwh-wazzup`.`dbt_nbespalov`.`stg_eventLogs`
  where promised_payment_start_date is not null 
  and promised_payment_end_date is not null
)


select                                                              
    accountId as account_id,                                -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    cast(payments.completedAt as date) as paid_date,        -- Дата перехода платежа в state = completed, формат 2022-11-29. Это происходит в случае подтверждения со стороны платежной системы, или когда мы сами проводим себе платеж и считаем что его можно везде учитывать
    payments.completedAt as paid_at,                        -- Дата и время перехода платежа в state = completed, формат 2022-11-29. Это происходит в случае подтверждения со стороны платежной системы, или когда мы сами проводим себе платеж и считаем что его можно везде учитывать
    payments.currency,                                      -- Валюта, возможные значения:RUR - рубли, USD - доллары, EUR - евро, KZT - тенге
    sum,                                                    -- Сумма оплаты. До 12 знаков до запятой, 2 после
    vat,                                                    -- Сумма налога. До 12 знаков до запятой, 2 после
    guid as guid,                                           -- Идентификатор оплаты.Генерируется Postgress при создании записи
    cast(json_value(payments.details,'$.partnerAccountId') as INTEGER) as partner_account_id, -- Идентификатор аккаунта партнера, соответствует id из таблицы stg_accounts (только при payment_provider = partner)
    json_value(payments.details,'$.provider') as payment_provider,                   -- Обозначает источник платежа: yandexkassa - Яндекс Касса, tinkoff - Тинькофф, stripe - Stripe, cashless - безнал, в этом месте не актуально, intellect_money/intellectMoney - устарело в связи с уходом от IntellectMoney, partner - Оплата с партнерского счета. При этом sum = 0, setPromisedPayment - Обещанный платеж. При этом sum = 0
    cast(json_value(payments.details,'$.isSbpPayment') as bool) as is_spb_payment,   -- Является ли платежом через систему быстрых платежей (СПБ)
    json_value(payments.details,'$.subscriptionUpdateId') subscription_update_id,    -- guid изменения подписки, заполняется если есть subscription_id. Соответствует guid из таблицы stg_subscriptionUpdates
    json_value(payments.details,'$.nextTimeAutoRenewal') as next_time_auto_renewal,  -- В следующий раз будет автопродление
    subscriptionId as subscription_id,                      -- Индентификатор подписки, соответствует полю guid из таблицы stg_billingPackages
    state,                                                  -- Статус платежа. Возможные значения: created платеж создан, completed платеж прошел, canceled платеж отменен
    payments.details,                                       -- JSON с различными полями, деталями подписки. Подробнее в yml
    promised_payment_type,                                  -- Тип обещанного платежа. Возможные значения: renewal, pay, addQuantity, raiseTariff
    promised_payment_end_date,                              -- Дата и время окончания обещанного платежа
    promised_payment_start,                                 -- Дата и время начала обещанного платежа
    datetime(date_add(cast(json_value(payments.details,'$.activeUntil') as timestamp),interval accounts.time_zone hour)) as active_until,
    datetime(date_add(cast(json_value(payments.details,'$.startDate') as timestamp),interval accounts.time_zone hour)) as payments_start_date 
    from `dwh-wazzup`.`wazzup`.`payments`  payments
    left join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts 
    on accounts.account_id=payments.accountId
    
    left join eventlogs 
    on TIMESTAMP_TRUNC(date_add(cast(json_value(payments.details,'$.startDate') as timestamp),interval accounts.time_zone hour), hour)=eventlogs.promised_payment_start_date
    and payments.subscriptionId=eventlogs.subject_id

/*
sum = 0 when state in ('created','completed')
created - платеж создан, но не оплачен
completed - платеж выполнен
sum = 0 , если это обещанынй платеж или партнерская оплата
*/