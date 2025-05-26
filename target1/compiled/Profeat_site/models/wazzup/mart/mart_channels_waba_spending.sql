WITH channels AS (
                 SELECT account_id
                      , guid
                      , phone
                 FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_channels`
                 WHERE transport = 'wapi'
                 ),

     waba_sessions AS (
                 SELECT chat_id
                      , initiator
                      , channel_id
                      , session_type
                      , session_id
                      , country
                      , state
                      , transaction_id
                 FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_sessions` sessions
                 WHERE state != 'canceled'
                   AND NOT EXISTS (
                     SELECT 1
                     FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_sessions_gupshup` gupshup
                     JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_sessions` core ON gupshup.session_id = core.session_id
                   AND CAST (gupshup.paid_at_waba_sessions AS DATE) = CAST (core.paid_at_waba_sessions AS DATE)
                     WHERE sessions.session_id = gupshup.session_id
                     ) -- это багованные сессии, пока решили их просто исключать 
                 ),

     waba_transactions AS (
                 SELECT amount
                      , date_at
                      , currency
                      , id
                 FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_transactions`
                 WHERE type = 'payment' -- берем списания денег
                 ),

     gupshup_spending_prep AS (
                 SELECT waba_transactions.total_amount   AS amount
                      , waba_sessions.created_at         AS date_at
                      --waba_transactions.date_at,
                      , waba_transactions.currency
                      , channels.account_id
                      , channels.phone                   AS phone
                      , waba_sessions.chat_id
                      , waba_sessions.country
                      , waba_sessions.channel_id
                      , waba_sessions.session_type
                      , waba_sessions.state
                      , row_number() OVER (PARTITION BY CASE WHEN waba_sessions.session_id IS NOT NULL
                                                                 THEN waba_sessions.session_id
                                                             ELSE cast(waba_transactions.id as string)
                     END
                     ORDER BY waba_transactions.id DESC) AS last_state_of_the_session
                 FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_transactions_gupshup` waba_transactions
                 JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_sessions_gupshup` waba_sessions ON waba_transactions.waba_session_id = waba_sessions.transaction_id
                 JOIN channels ON waba_sessions.channel_id = channels.guid
                 WHERE waba_sessions.state != 'canceled'
                   AND waba_transactions.type = 'payment'
                   

                   union all

            -- Добавляем пользователей, чей channel_id мы не смогли идентифицировать (нет в гейте, либо мы неверно соединяем таблицы)
                    SELECT waba_transactions.total_amount   AS amount
                      , waba_sessions.created_at         AS date_at
                      --waba_transactions.date_at,
                      , waba_transactions.currency
                      , channels.account_id
                      , channels.phone                   AS phone
                      , waba_sessions.chat_id
                      , waba_sessions.country
                      , waba_sessions.channel_id
                      , waba_sessions.session_type
                      , waba_sessions.state
                      , row_number() OVER (PARTITION BY CASE WHEN waba_sessions.session_id IS NOT NULL
                                                                 THEN waba_sessions.session_id
                                                             ELSE cast(waba_transactions.id as string)
                     END
                     ORDER BY waba_transactions.id DESC) AS last_state_of_the_session
                 FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_transactions_gupshup` waba_transactions
                 JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_sessions_gupshup` waba_sessions ON waba_transactions.waba_session_id = waba_sessions.transaction_id
                 left JOIN channels ON waba_sessions.channel_id = channels.guid
                 WHERE waba_sessions.state != 'canceled'
                   AND waba_transactions.type = 'payment'
                   and channels.guid is null
                
                 --where waba_sessions.session_id is not null в гупшупе у захолдированных сессий нет session_id
                 ),
     gupshup_spending AS (
                 SELECT account_id
                      , channel_id
                      , phone
                      , chat_id
                      , cast(NULL AS string) AS initiator --legacy, не используется
                      , country
                      , session_type
                      , state
                      , date_at
                      , amount
                      , currency
                 FROM gupshup_spending_prep
                 WHERE last_state_of_the_session = 1
                         --and amount != 0
                 ),
     waba_spending AS (
                 SELECT channels.account_id
                      , channels.guid                         AS channel_id
                      , channels.phone
                      , cast(waba_sessions.chat_id AS string) AS chat_id
                      , waba_sessions.initiator
                      , waba_sessions.country
                      , waba_sessions.session_type
                      , waba_sessions.state
                      , waba_transactions.date_at
                      , waba_transactions.amount
                      , waba_transactions.currency
                 FROM channels
                     INNER JOIN waba_sessions
                             ON channels.guid = waba_sessions.channel_id
                     INNER JOIN waba_transactions
                             ON waba_sessions.transaction_id = waba_transactions.id
                 WHERE NOT (amount = 0 AND session_id IS NULL)

                 UNION ALL

                 SELECT account_id      -- ID аккаунта
                      , channel_id      -- Идентификатор канала, соответсвует guid из stg_channels
                      , phone           -- Номер телефона, на котором создана ваба
                      , chat_id         -- Телефон контакта (контакт определяется как chatType-chatId. Здесь chatType = 'wapi')
                      , initiator       -- Инициатор сессии из фейсбука. Нужно для определения стоимости сессии. Возможные варианты: 'business' | 'contact' | 'free'
                      , country         -- Страна контакта. Нужна для определения стоимости сессии
                      , session_type    -- Тип сессии, определенный фейсбуком в рамках тарификации Возможные варианты: 'utility' | 'authentication' | 'marketing' | 'service' | 'referral_conversion'
                      , state           -- Состояние оплаты сессии, возможные варианты: 'holded' | 'paid' | 'canceled'
                      , date_at         -- Дата и время проведения транзакции, формат 2022-11-29T19:49:52.778Z
                      , amount          -- Сумма пополнения/списания
                      , currency        -- Валюта, как и в ЛК. В целом может отличаться в разных записях, если у пользователя была смена валюты ЛК; RUR - рубли, USD - доллары, EUR - евро, KZT - тенге
                 FROM gupshup_spending

                 ),
     profile_info AS (
                 SELECT account_id
                 FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
                 WHERE is_employee IS TRUE
                   AND account_id != 54963500 --аккаунт саппорта, попросили добавить https://wazzup.planfix.ru/task/1135629/?comment=194165636
                 )
    -- Таблица со списаниями с баланса вабы клиентов за определенный период
SELECT *
FROM waba_spending
WHERE NOT exists
          (
          SELECT account_id
          FROM profile_info
          WHERE waba_spending.account_id = profile_info.account_id
          )