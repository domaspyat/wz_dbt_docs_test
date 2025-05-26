select          -- Эта таблица хранит данные по всем списаниям за сессии из таблицы stg_channels_waba_transactions
    chatId as chat_id,                      -- Телефон контакта
    initiator,                              -- Инициатор сессии из фейсбука
    channelId as channel_id,                -- guid канала, соответствует guid из stg_channels
    state,                                  -- Состояние оплаты сессии
    transactionId as transaction_id,        -- Идентификатор транзакции, соответствует id в таблице stg_channels_waba_transactions
    country,                                -- Страна контакта
    sessionType as session_type,            -- Тип сессии, определенный фейсбуком в рамках тарификации
    sessionId as session_id,                -- Идентификатор 24х часовой сессии из фейсбука, т.е. за эту сессию списаны деньги
    paidat as paid_at_waba_sessions         -- Дата и время оплаты сессии
from `dwh-wazzup`.`wazzup`.`wabaSessions`