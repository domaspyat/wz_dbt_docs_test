select          -- Отражает движения средств на балансе ваба.
    dateTime as date_at,                        -- Дата и время проведения транзакции
    _ibk as transaction_date,                   -- Дата проведения транзакции
    id,                                         -- Идентификатор транзакции
    amount,                                     -- Сумма пополнения/списания
    type,                                       -- Тип тразакции:
    currency,                                   -- Валюта, как и в ЛК
    subscriptionId as subscription_id           -- guid подписки, соответствует guid из stg_billingPackages
from `dwh-wazzup`.`wazzup`.`wabaTransactions`
where id!=47231455      --*видимо, заведомо неверная транзакция*