with channels as (
    select account_id,
    phone,
    guid,
    transport
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels`),

waba_sessions as (
    select chat_id,
    initiator,
    channel_id,
    transaction_id,
    state from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_sessions`),

waba_transactions as (
    select date_at,
    id,
    amount,
    type from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_transactions`
),
waba_balance_spending as (
    select
    channels.account_id,        -- ID аккаунта
    channels.phone,             -- Номер канала WABA
    waba_sessions.chat_id,      -- Номер телефона получателя сообщения
    waba_sessions.initiator,    -- Инициатор сессии
    waba_transactions.date_at,  -- Дата и время сессии
    waba_transactions.amount    -- Сумма списания по сессии
    from channels
    inner join waba_sessions on waba_sessions.channel_id=channels.guid
    inner join waba_transactions on waba_transactions.id=waba_sessions.transaction_id
    where channels.transport='wapi'
    and waba_sessions.state != 'canceled' 
    and  waba_transactions.type='payment'
)   -- Таблица c каналами WABA и тратами по ним
select * from waba_balance_spending