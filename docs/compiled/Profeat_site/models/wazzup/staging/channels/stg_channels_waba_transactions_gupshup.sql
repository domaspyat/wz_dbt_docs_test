select                               -- Эта таблица отражает движение средств на балансе WABA с официальным провайдером GupShup. 
        date_time               AS date_at,                                 -- Дата и время транзкации
        _ibk                    AS transaction_date,                        -- Дата транзакции     
        cast(id AS int)         AS id,                                      -- id транзакции
        amount,                                                             -- Сумма транзакции
        total_amount,                                                       -- Конечная сумма списания после корректировок
        reason,                                                             -- Причина транзакции: paySession, changeCurrency, cancelSession, correctionAmount или NULL
        type,                                                               -- Тип транзакции: batch, topup, withdrawal, payment, convertation
        currency,                                                           -- Валюта: EUR, USD, RUR, KZT
        service_subscription_id AS subscription_id,                         -- id подписки, соотвествует subscription_id is stg_subscriptionUpdates
        waba_session_id,                                                    -- Ключ для связи с waba_sessions_gupshup,
        waba_subscription_id,                                               -- Ключ для связи с waba_subscription_gupshup
        application_guid                                                    -- id приложения, для связи с gupshup
from `dwh-wazzup`.`wazzup`.`wabaTransaction_gupshup`