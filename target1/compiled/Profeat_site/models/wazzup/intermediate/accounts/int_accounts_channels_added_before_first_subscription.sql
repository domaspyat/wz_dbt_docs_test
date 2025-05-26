with first_channels_by_transport as (
    select *,
    cast(channel_created_at as date) as channel_created_date
     from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_first_channel_added__by_transport`),

first_subscription as (
    select account_id,
    start_date as subscription_start from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_first_subscription_date_and_type`
),

registration_info as (
    select account_id, 
    date_add(register_date, INTERVAL 21 DAY) as  register_date
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
),

channels_before_first_subscription as (
    select first_channels_by_transport.account_id, 
    first_channels_by_transport.transport, 
    coalesce(subscription_start,register_date) as typical_date,
    channel_created_date
    from first_channels_by_transport 
    left join first_subscription
    on first_channels_by_transport.account_id=first_subscription.account_id
    left join
    registration_info
    on first_subscription.account_id=registration_info.account_id
    order by account_id, transport),

first_transport_before_subscription as (
    select account_id,                              -- ID аккаунта
    string_agg(transport, ",") as transport_added   -- Добавленные каналы
    from channels_before_first_subscription
    where channel_created_date<=typical_date
    group by 1)
    -- Таблица с каналами, добавленными до первой подписки по аккаунтам
select * from  first_transport_before_subscription