with unsubscribe_info as (
    select *, 
    cast(deleted_at as date) as deleted_date
     from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_deleted_from_eventLogs`
),  -- Таблица с информацией об удаленных подписках

subscription_data as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_old_and_new_end_date_filledna_with_tariff_and_transport`
    where end_date is not null
),  -- Таблица подписок, которые закончились

subscription_with_deleted as (   
        select account_id,                              -- ID аккаунта
        start_date,                                     -- Дата начала подписки
        subscription_data.subscription_id,              -- ID подписки
        start_at,                                       -- Дата и время начала подписки
        (case when deleted_date is null then end_date   
        when end_date<deleted_date then end_date
        else deleted_date
        end) as end_date,                               -- Дата окончания подписки с условиями
        action,                                         -- Действие с подпиской: addQuantity, setPromisedPayment, pay, subtractQuantity, templateMessages, loweringTariff, raiseTariff, renewal, balanceTopup или null
        partner_account_id,                             -- ID акканута партнера
        tariff,                                         -- Тариф
        transport                                       -- Транспорт
        from subscription_data
        left join unsubscribe_info
        on subscription_data.subscription_id=unsubscribe_info.subscription_id),
    -- Таблица удаленных подписок с данными о действии и партнере
subscription_with_deleted_to_deduplicate as (
    select *,
    row_number() over (partition by account_id, subscription_id, start_date order by end_date desc) rn  -- Ранг по партиации
    from subscription_with_deleted
),

subscription_with_deleted_deduplicated as (
    select * from subscription_with_deleted_to_deduplicate
    where rn=1
),

subscription_with_deleted_date as (
 
select account_id,                                      -- ID аккаунта
        start_date,                                     -- Дата начала подписки
        start_at,                                       -- Дата и время начала подписки
        subscription_id,                                -- ID подписки
        end_date,                                       -- Дата окончания подписки с условиями
        action,                                         -- Действие с подпиской: addQuantity, setPromisedPayment, pay, subtractQuantity, templateMessages, loweringTariff, raiseTariff, renewal, balanceTopup или null
        partner_account_id,                             -- ID аккаунта партнера
        tariff,                                         -- Тариф
        transport                                       -- Транспорт
        from subscription_with_deleted_deduplicated)
select * from subscription_with_deleted_date -- Таблица подписок и действий с ними, если у них есть дата удаления