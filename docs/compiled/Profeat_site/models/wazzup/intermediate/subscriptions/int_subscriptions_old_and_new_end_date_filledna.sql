



with end_date_r_close as (
    select *, 
    sum(case when end_date is not null then 1 else 0 end) over (partition by subscription_id
    order by start_at) as r_close
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_old_and_new_data_union`
),

end_date_r_close_filled as (

    select
    *, 
    first_value(end_date) over (partition by subscription_id,
    r_close order by start_at rows between unbounded preceding and unbounded following) as end_date_filled
    from end_date_r_close
   ),

unsubscribe_info as (
    select *, 
    cast(deleted_at as date) as deleted_date
     from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_deleted_from_eventLogs`
),

subscription_data as (
    select * from  end_date_r_close_filled
    where end_date_filled is not null
),

subscription_with_deleted as (   
        select account_id,                                      -- ID аккаунта
        start_date,                                             -- Дата начала подписки
        subscription_data.subscription_id,                      -- ID подписки
        start_at,                                               -- Дата и время начала подписки
        (case when deleted_date is null then end_date_filled    
        when end_date_filled<deleted_date then end_date_filled
        else deleted_date
        end) as end_date,                                       -- Дата окончания подписки с условиями
        action,                                                 -- Действие с подпиской: addQuantity, setPromisedPayment, pay, subtractQuantity, templateMessages, loweringTariff, raiseTariff, renewal, balanceTopup или null
        partner_account_id,                                     -- ID аккаунта партнера
        currency,                                               -- Валюта
        guid                                                    -- guid изменения - соответствует guid из subscriptionUpdates
        from subscription_data
        left join unsubscribe_info
        on subscription_data.subscription_id=unsubscribe_info.subscription_id),

subscription_with_deleted_date as (

select account_id,                                              -- ID аккаунта
        start_date,                                             -- Дата начала подписки
        start_at,                                               -- Дата и время начала подписки
        subscription_id,                                        -- ID подписки
        end_date,                                               -- Дата окончания подписки с условиями
        action,                                                 -- Действие с подпиской: addQuantity, setPromisedPayment, pay, subtractQuantity, templateMessages, loweringTariff, raiseTariff, renewal, balanceTopup или null
        partner_account_id,                                     -- ID аккаунта партнера
        currency,                                               -- Валюта
        guid                                                    -- guid изменения - соответствует guid из subscriptionUpdates
        from subscription_with_deleted)



 select * from subscription_with_deleted_date   -- Таблица с данными об изменении подписки