select          -- Таблица истории баланса средств в ЛК.Также используется для вычисления текущего баланса
    case when accountId = 60569941 then 28266449 else accountid end as account_id,          -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts.
    guid,                                                                                   -- Автогенерируемый идентификатор записи.
    id AS ba_id,                                                                            -- ID записи из billingAffiliate
    cast(dateTime as date) as occured_date,                                                 -- Дата создания события
    cast(json_value(details,'$.subscriptionOwner') as integer) as subscription_owner,       -- Номер аккаунта кому принадлежит подписка, за которую платили. Соответствует id из таблицы stg_accounts.
    json_value(details,'$.subscriptionUpdateId') as subscription_update_id,                 -- Идентификатор записи об изменении подписки, соответствует guid из таблицы stg_subscriptionUpdates.
    dateTime as occured_at,                                                                 -- Дата и время создания события.
    currency,                                                                               -- Валюта, в которой была осуществлена оплата
    abs(sum) as sum,                                                                        -- Сумма оплаты по модулю
    sum as original_sum,                                                                    -- Сумма оплаты
    object,                                                                                 -- Тип транзакции
    method,                                                                                 -- Способ проведение платежа
    cast(json_value(details, '$.invalid') as bool) as is_invalid,                           -- Актуально при оплате счетов. Если оплата была с помощью невалидного счета, то true, иначе false. При оплате с невалидного счета деньги зачисляются на бонусный счет аккаунта.
    cast(json_value(details, '$.paymentGuid') as string) as payment_guid,                   -- Идентификатор платежа, если платеж был осуществлен картой. Соответствует guid из таблицы stg_payments_card
    cast(json_value(details, '$.invoiceId') as string) as invoice_id                        -- Идентификатор платежа, если платеж был осуществлен по безналу. Соответствует id из таблицы stg_payments_bills
from `dwh-wazzup`.`wazzup`.`billingAffiliate`
where not (accountId=96674295 and object='takeAway' and cast(dateTime as date)=cast(timestamp('2024-01-27') as date)) --эти изменения делались в рамках правок найденных расхождений в январе https://wazzup.planfix.ru/task/1105536