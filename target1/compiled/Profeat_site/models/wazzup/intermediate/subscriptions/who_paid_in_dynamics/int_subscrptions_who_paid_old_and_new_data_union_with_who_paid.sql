with old_and_new_data_union as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_deleted_date`
    where action in ('pay','renewal','subtractQuantity','setPromisedPayment','addQuantity','raiseTariff') or action is null
),

old_and_new_data_with_segments as (
    select old_and_new_data_union.*, (case when action in ('setPromisedPayment','subtractQuantity') then null
    when partner_account_id is null then 'self'
    else 'partner' 
    end) as who_paid
    from old_and_new_data_union 
    ),

subscription_updates_to_fill as (
    select *, sum(case when who_paid is not null then 1 end) over (partition by account_id order by start_at)
    as r_close from old_and_new_data_with_segments),

subscription_filled as (
    select *,
    first_value(who_paid) over (partition by account_id,
    r_close order by start_at asc) as who_paid_filled from 
    subscription_updates_to_fill
    ),

 subscription_filled_who_paid as (   
select account_id,                                  -- ID аккаунта
start_at,                                           -- Дата и время начала подписки
start_date,                                         -- Дата начала подписки
end_date,                                           -- Дата окончания подписки
subscription_id,                                    -- ID подписки
partner_account_id,                                 -- ID аккаунта партнера, если платил партнер
action,                                             -- Действие с подпиской: setPromisedPayment, subtractQuantity, pay, addQuantity, raiseTariff, renewal
coalesce(who_paid, who_paid_filled) as who_paid     -- Кто платил за подписку: self, partner, null
from subscription_filled
where account_id is not null)
    -- Таблица, которая показывает активность клиента по подпискам, а также показывает кто платил за его подписки
select * from subscription_filled_who_paid