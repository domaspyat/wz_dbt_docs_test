select                                 -- Таблица, которая показывает когда у аккаунта было первое сообщение
f0_ as min_messages_at,                         -- Дата и время первого сообщения
cast(f0_ as date) as min_message_date,          -- Дата первого сообщения
accountId as account_id                         -- Идентификатор аккаунта
from `dwh-wazzup`.`wazzup`.`minIncomingMessage`