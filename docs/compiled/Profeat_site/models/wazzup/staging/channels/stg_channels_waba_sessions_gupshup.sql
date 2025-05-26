select                                           -- Эта таблица хранит данные по всем списаниям за сессии из таблицы stg_channels_waba_transactions_gupshup. Gupshup - официальный провайдер ВАБА
        phone as chat_id,                                                -- Номер телефона получателя сообщений
        null as initiator,                                               -- Пустое поле для связи с stg_channels_waba_sessions
        coalesce(gate.subscription_id,application_guid) as channel_id,   -- id канала или id приложения gupshup, если id канала = NULL
        state,                                                           -- Статус  сессии: paid, holded, canceled
        id as transaction_id,                                            -- id транзакции для связи с stg_channels_waba_transactions_gupshup
        country,                                                         -- Страна: код из страны из двух букв. Например, RU (РФ) или BG (Болгария)
        type as session_type,                                            -- Тип шаблона: marketing, service, utility, FEP, FTC, authentication
        conversation_id as session_id,                                   -- id сессии
        paid_at as paid_at_waba_sessions,                                -- Дата и время оплаты сессии
        created_at,                                                      -- Дата и время создания записи   
        _ibk as created_date                                             -- Дата создания сессии
from `dwh-wazzup`.`wazzup`.`wabaSessions_gupshup_new` waba
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_gate_channels` gate on waba.application_guid = gate.channel_id