with bills_and_account_type as (
    select 
        bills.account_id,    -- ID аккаунта
        bills.paid_date,                -- Дата оплаты
        coalesce(bills.completed_at, cast(bills.paid_date as timestamp)) as paid_at,     -- Дата и время оплаты счета. У неготорых счетов не выставляется completed_at, но оплата прошла успешно (05.03.2024)
        bills.currency,                 -- Валюта
        bills.sum_in_rubles,            -- Сумма оплаты в рублях
        wapi_transactions_in_rubles,    -- Сумма оплаты баланса WABA в рублях
        wapi_original_sum,              -- Сумма оплаты баланса WABA
        bills.original_sum,             -- Сумма оплаты
        bills.guid,                     -- Идентификатор счета. Генерируется Postgress при создании записи в формате string
        bills.subscription_id,          -- ID подписки
        bills.updated_at,               -- Дата и время обновления счета
        billing_date_subscription_start as completed_date,      -- Дата оплаты счета
        subscription_updates.guid as subscriptionupdates_guid,  -- ID изменения. Соответствует guid из subscriptionUpdates
        subscription_updates.action,    -- Действие с подпиской
        subscription_updates.sum_in_rubles as sum_in_rubles_full_subscription,  -- Сумма в рублях, заплаченная за изменение
        subscription_updates.original_sum as subscription_updates_original_sum, -- Сумма из subscriptionUpdates
        account_type_data.account_type, -- Тип аккаунта
        partner_discount,   -- Скидка партнера
        start_date,         -- Дата регистрации аккаунта
        (case when partner_discount is not null and ((bills.paid_date>='2022-11-28'  and account_type='partner') or (bills.paid_date>='2023-02-10' and account_type='tech-partner')) then 0.1*wapi_transactions_in_rubles end) as wapi_discount_for_partners, --скидку для оф. партнеров ввели 2022-11-28, для тех. партнеров 2023-02-10
        subscription_updates.sum_in_rubles-coalesce(subscription_updates.wapi_transactions_in_rubles,0) as subscription_sum_in_rubles,    -- Сумма оплаты подписки в рублях
        subscription_updates.original_sum-coalesce(subscription_updates.wapi_original_sum,0) as subscription_sum_original                 -- Сумма оплаты подписки
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_bills` bills
    left join `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency_billing_date` subscription_updates
    on bills.guid=subscription_updates.activation_reason_id
    inner join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` account_type_data 
    on account_type_data.account_id=bills.account_id and  bills.paid_date>=account_type_data.start_date and bills.paid_date<=account_type_data.end_date
    where status='paid'
    and id!=101910),

bills_and_account_type_to_deduplicate as (
select *, row_number() over (partition by subscriptionupdates_guid order by paid_date, start_date desc) rn from  bills_and_account_type  )
    -- Таблица валидных счетов
select * from bills_and_account_type_to_deduplicate
where rn=1  -- Берутся только самые поздние изменения по subscriptionUpdates.guid