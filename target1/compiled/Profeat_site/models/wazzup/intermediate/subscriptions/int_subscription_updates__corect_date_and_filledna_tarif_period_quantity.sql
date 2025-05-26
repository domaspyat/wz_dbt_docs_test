with subscription_updates_fillna_start as (
     select *,
                sum(case when period is not null then 1 end) over (partition by subscription_id
                order by created_at) as period_close,
                sum(case when quantity is not null then 1 end) over (partition by subscription_id
                order by created_at) as quantity_close,
                sum(case when tariff is not null then 1 end) over (partition by subscription_id
                order by created_at) as tariff_close,
                sum(case when old_period is not null then 1 end) over (partition by subscription_id
                order by created_at) as old_period_close,
                sum(case when old_quantity is not null then 1 end) over (partition by subscription_id
                order by created_at) as old_quantity_close,
                sum(case when old_tariff is not null then 1 end) over (partition by subscription_id
                order by created_at) as old_tariff_close,
                sum(case when partner_discount is not null then 1 end) over (partition by subscription_id
                order by created_at) as partner_discount_close
                from `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates`  where state='activated' 
),

payments_bills as (
    select guid,                                                
    account_id,
    paid_date,
    completed_at,
    updated_at,
    billing_date_subscription_start
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_bills`
),

payments_cards as (
    select guid,
    paid_at,
    account_id,
    partner_account_id,
    active_until,
    promised_payment_type,
    payment_provider
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_card`
    ),

subscription_updates_fillna_tarif_period_quantity as (
    select guid,
    subscription_id,
    activation_reason_id,
    activation_object,
    balance_to_withdraw,
    wapi_transactions,
    created_at,
    sum,
    created_date,
    currency,
    action,
    until_expired_days,
    new_until_expired_days,
    partner_discount,
    --эти поля нужны для определения периода подписок в случае, если только пополняется баланс вабы
    cast(first_value(period) over (partition by subscription_id, period_close  order by created_at rows between unbounded preceding and unbounded following ) as integer) as period_new,
    cast(first_value(quantity) over (partition by subscription_id, quantity_close order by created_at rows between unbounded preceding and unbounded following) as integer) as quantity_new,
    first_value(tariff) over (partition by subscription_id, tariff_close order by created_at rows between unbounded preceding and unbounded following) as tariff_new,
    cast(first_value(old_period) over (partition by subscription_id, old_period_close  order by created_at rows between unbounded preceding and unbounded following ) as integer) as period_old,
    cast(first_value(old_quantity) over (partition by subscription_id, old_quantity_close order by created_at rows between unbounded preceding and unbounded following) as integer) as quantity_old,
    first_value(old_tariff) over (partition by subscription_id, old_tariff_close order by created_at rows between unbounded preceding and unbounded following) as tariff_old,
    first_value(partner_discount) over (partition by subscription_id, partner_discount_close order by created_at rows between unbounded preceding and unbounded following) as partner_discount_new
    from subscription_updates_fillna_start
),

subscription_updates_fillna_tarif_period_quantity_with_correct_dates as (
    select                        -- заполненные данные для subscriptionUpdates. не для всех action заполняется period, quantity,tarif
    subscriptions.guid,                                             -- id изменения
    subscriptions.subscription_id,                                  -- id подписки
    subscriptions.activation_reason_id,                             -- тут док акт ризон ид
    subscriptions.activation_object,                                -- тут док актив обжект
    subscriptions.balance_to_withdraw,                              -- в случае частичной оплаты кол-во бонусов клиента используемых для оплаты подписки
    subscriptions.wapi_transactions,                                -- сумма на которую пополняем waba баланс
    subscriptions.created_at,                                       -- время создания записи
    subscriptions.sum,                                              -- полная стоимость подписки с учетом только скидки на длительность. партнерская скидка и сумма списания с баланса не учитывается
    subscriptions.created_date,                                     -- дата создания записи
    subscriptions.currency,                                         -- валюта
    (case when subscriptions.action is null and payments_cards.payment_provider='setPromisedPayment'
    then 'setPromisedPayment'
    else subscriptions.action
    end) as action,                                                 -- тут док экшн
    subscriptions.until_expired_days,                               -- количество дней до окончания на момент создания изменения
    subscriptions.new_until_expired_days,                           -- количество дней до окончания на момент применения изменения
    subscriptions.partner_discount,                                 -- % партнерской скидки на момент создания изменения
    subscriptions.period_new,                                       -- Новый период подписки
    subscriptions.quantity_new,                                     -- Новое кол-во каналов
    subscriptions.tariff_new,                                       -- Новый тариф подписки
    subscriptions.period_old,                                       -- период подписки на момент создания изменения
    subscriptions.quantity_old,                                     -- кол-во каналов на момент создания изменения
    subscriptions.tariff_old,                                       -- тариф подписки на момент создания изменения
    subscriptions.partner_discount_new,                             -- заполненный % партнерский скидки -- //todo проверить, где используется и нафиг нужен
    payments_cards.partner_account_id,                              -- если подписки оплатил партнер - partner_account_id партнера
    (case when promised_payment_type in ('raiseTariff','addQuantity') then null else  active_until end) as active_until, -- время окончания обещанного платежа по продлению подписки
       (case 
        when activation_reason_id='180' then cast('2021-10-19' as datetime)
        when activation_reason_id='15535' then cast('2022-07-05' as datetime)
        when activation_reason_id='8112' then cast('2022-03-28' as datetime)
        when activation_reason_id='8629' then cast('2022-03-30' as datetime)
        when activation_object='invoice' and payments_bills.updated_at is not null then cast(payments_bills.updated_at as datetime)
        when activation_object='invoice' and payments_bills.completed_at is not null and payments_bills.updated_at is null then cast(payments_bills.completed_at as datetime)    
        when activation_object='invoice' and payments_bills.updated_at is null and payments_bills.paid_date is not null and payments_bills.completed_at is null then cast(payments_bills.paid_date as datetime)
        when activation_object in ('payment','partnerBalance') and payments_cards.paid_at is not null then cast(payments_cards.paid_at as datetime)
		else subscriptions.created_at end) as paid_at,              -- дата и время оплаты
    (case 
        when activation_reason_id='180' then cast('2021-10-19' as datetime)
        when activation_reason_id='15535' then cast('2022-07-05' as datetime)
        when activation_reason_id='8112' then cast('2022-03-28' as datetime)
        when activation_reason_id='8629' then cast('2022-03-30' as datetime)
        when activation_object='invoice' and  payments_bills.paid_date is not null then cast(payments_bills.paid_date   as datetime)
        when activation_object='invoice' and payments_bills.updated_at is not null then cast(payments_bills.updated_at as datetime)    
        when activation_object='invoice' and payments_bills.completed_at is not null then cast(payments_bills.completed_at as datetime)
        when activation_object in ('payment','partnerBalance') and payments_cards.paid_at is not null then cast(payments_cards.paid_at as datetime)
		else subscriptions.created_at end) as paid_at_billing,      -- дата и время оплаты по биллингу

     (case 
        when activation_reason_id='180' then cast('2021-10-19' as datetime)
        when activation_reason_id='15535' then cast('2022-07-05' as datetime)
        when activation_reason_id='8112' then cast('2022-03-28' as datetime)
        when activation_reason_id='8629' then cast('2022-03-30' as datetime)
        when activation_object='invoice' and payments_bills.billing_date_subscription_start is not null then payments_bills.billing_date_subscription_start  
        when activation_object='invoice' and payments_bills.updated_at is not null then cast(payments_bills.updated_at as datetime)  
        when activation_object='invoice' and payments_bills.completed_at is not null then cast(payments_bills.completed_at as datetime)
        when activation_object='invoice' and  payments_bills.paid_date is not null then cast(payments_bills.paid_date   as datetime)
          
        
        when activation_object in ('payment','partnerBalance') and payments_cards.paid_at is not null then cast(payments_cards.paid_at as datetime)
		else subscriptions.created_at end) as paid_at_billing_completed_at --примерно совпадает с датой применения изменения в продукте в случае платежа безналом. чаще всего все 3 времени совпадают


     from subscription_updates_fillna_tarif_period_quantity subscriptions
    left join payments_bills on subscriptions.activation_reason_id=payments_bills.guid
	left join payments_cards on subscriptions.activation_reason_id=payments_cards.guid
    left join wazzup.accounts accounts on accounts.id=coalesce(payments_bills.account_id,payments_cards.account_id) 
    )    
select *, 
cast(paid_at as date) as paid_date,                                             -- дата оплаты
 cast(paid_at_billing as date) as paid_at_billing_date,                         -- дата оплаты по биллингу
 cast(paid_at_billing_completed_at as date) as paid_at_billing_completed_date   -- дата завершения оплаты. чаще всего все 3 времени совпадают
from subscription_updates_fillna_tarif_period_quantity_with_correct_dates