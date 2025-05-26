with balance_spending as (
    select 
        account_id as partner_id,
        occured_date as paid_date,
        occured_at as paid_at,          -- (04.03.2025) 
        billing_affiliate_original_sum as billing_affiliate_original_sum_paid_by_partner,
        subscriptionupdates_original_sum,
        wapi_original_sum,
        subscription_owner as account_id, 
        billingaffiliate_currency as billingaffiliate_currency_partner,
        subscription_updates_currency as subscription_updates_currency_partner,
        occured_date, 
        subscription_id,  
        subscription_update_id,
        action,
        partner_discount as partner_discount,
        good_balance_spent as good_balance_spent, 
        wapi_transactions_in_rubles as wapi_transactions_in_rubles,
        sum_in_rubles_full_subscription as sum_in_rubles_full_subscription,
        sum_in_rubles_full_subscription-wapi_transactions_in_rubles as subscription_sum_in_rubles,
        subscriptionupdates_original_sum-wapi_original_sum as subscription_sum_original,
        sum_in_rubles,
        account_type
        from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_billing_affiliate_subscription_payments_real_money_with_who_paid`        
        where has_partner_paid=2
        ),

revenue_union as ( 
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_and_wapi_sessions_bills_and_cards`
    ),

revenue_joined_with_balance as (
    select revenue_union.data_source,                                                               -- Источник оплаты
    coalesce(revenue_union.account_id, balance_spending.account_id) as account_id,                  -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    coalesce(cast(revenue_union.paid_at as datetime), balance_spending.paid_at) as paid_at,                   -- Дата и время оплаты (05.04.2025)
    coalesce(revenue_union.paid_date, balance_spending.paid_date) as paid_date,                     -- Дата оплаты
    coalesce(revenue_union.sum_in_rubles,0) as card_or_bills_sum_in_rubles,                         -- Сумма, заплаченная реальными деньгами в рублях
    coalesce(round(revenue_union.original_sum,0)) as card_or_bills_original_sum,                    -- Сумма, заплаченная реальными деньгами
    coalesce(revenue_union.subscription_id, balance_spending.subscription_id) as subscription_id,   -- ID подписки
    coalesce(revenue_union.action, balance_spending.action) as action,                              -- Действие с подпиской
    coalesce(revenue_union.subscription_update_id, balance_spending.subscription_update_id) as subscription_update_id,                                  -- ID изменения, соответствует guid из subscriptionUpdates
    coalesce(revenue_union.wapi_transactions_in_rubles, balance_spending.wapi_transactions_in_rubles) as wapi_transactions_in_rubles,                   -- Сумма денег, потраченных на баланс WABA в рублях
    coalesce(revenue_union.wapi_original_sum, balance_spending.wapi_original_sum) as wapi_original_sum,                                                 -- Сумма денег, потраченных на баланс WABA
    coalesce(revenue_union.subscription_sum_in_rubles, balance_spending.subscription_sum_in_rubles) as subscription_sum_in_rubles,                      -- Стоимость подписки в рублях
    coalesce(revenue_union.sum_in_rubles_full_subscription, balance_spending.sum_in_rubles_full_subscription) as sum_in_rubles_full_subscription,       -- Полная стоимость подписки в рублях
    coalesce(revenue_union.subscription_sum_original, balance_spending.subscription_sum_original) as subscription_sum_original,                         -- Стоимость подписки
    coalesce(balance_spending.billing_affiliate_original_sum_paid_by_partner,0) as balance_sum_original,                                                -- Сумма потраченных бонусов
    coalesce(balance_spending.sum_in_rubles,0) as balance_sum_in_rubles,                            -- Сумма потраченных бонусов в рублях
    coalesce(revenue_union.subscription_updates_original_sum, balance_spending.subscriptionupdates_original_sum) as subscription_updates_original_sum,  -- Сумма подписки из subscriptionUpdates
    coalesce(revenue_union.partner_discount, balance_spending.partner_discount) as partner_discount,-- Скидка партнера
    coalesce(balance_spending.good_balance_spent ,0) as good_balance_spent,                         -- Сумма потраченных хороших бонусов
    coalesce(revenue_union.account_type,balance_spending.account_type) as account_type,             -- Тип аккаунта
    coalesce(revenue_union.currency,balance_spending.billingaffiliate_currency_partner) as currency -- Валюта
    from revenue_union full outer join balance_spending 
    on revenue_union.subscription_update_id=balance_spending.subscription_update_id),

balance_and_payments as (
    select *,  
    (
        case when partner_discount is not null and ((paid_date>='2022-11-29'  and account_type='partner') or (paid_date>='2023-02-10' and account_type='tech-partner')) 
        then 0.1*wapi_original_sum
        else 0 end) as wapi_discount_for_partners_original, -- Комиссия партнера за пополнение баланса WABA
        ceil(subscription_sum_original*(1- coalesce(partner_discount,0))) as subscripion_sum_with_discount_original -- Сумма подписки без скидки
        from revenue_joined_with_balance
    ),

balance_and_payments_with_balance_spent as (
    select *, 
    coalesce((case 
    when subscripion_sum_with_discount_original=0 then 0
    when balance_sum_original=0 then 0
    when subscripion_sum_with_discount_original>=balance_sum_original then balance_sum_original
    else subscripion_sum_with_discount_original
    end
    ),0) as balance_spent_on_subscription_original,                     -- Сумма бонусов потраченных на подписку
    (case 
    when subscripion_sum_with_discount_original=0 then 0
    when subscripion_sum_with_discount_original>=coalesce(balance_sum_original,0) then subscripion_sum_with_discount_original-coalesce(balance_sum_original,0)
    when balance_sum_original>subscripion_sum_with_discount_original then 0
    end
    ) as subscription_sum_without_balance_spent_by_client_original,     -- Стоимость подписки без бонусов клиента
    balance_sum_original-coalesce((case 
    when subscripion_sum_with_discount_original=0 then 0
    when balance_sum_original=0 then 0
    when subscripion_sum_with_discount_original>=balance_sum_original then balance_sum_original
    else subscripion_sum_with_discount_original
    end
    ),0) as balance_spent_on_waba_original,                             -- Сумма бонусов потраченных на баланс WABA
    ceil((wapi_original_sum-wapi_discount_for_partners_original)+subscripion_sum_with_discount_original) as original_sum_to_pay -- Сумма, которую нужно заплатить для оплаты подписки
    from balance_and_payments
    ),
balance_and_payments_with_converted_sums as (
    select balance_and_payments_with_balance_spent.*,
    (
        case when balance_and_payments_with_balance_spent.currency='RUR' then subscripion_sum_with_discount_original
        else subscripion_sum_with_discount_original*rur
        end
    ) as subscripion_sum_with_discount_sum_in_rubles,                   -- Сумма оплаты подписки без скидки партнера в рублях
    (
        case when balance_and_payments_with_balance_spent.currency='RUR' then wapi_discount_for_partners_original
        else wapi_discount_for_partners_original*rur
        end
    ) as wapi_discount_for_partners_sum_in_rubles,                      -- Комиссия партнера за пополнение баланса WABA в рублях
    (
        case when balance_and_payments_with_balance_spent.currency='RUR' then ceil(wapi_original_sum-wapi_discount_for_partners_original)
        else ceil(wapi_original_sum-wapi_discount_for_partners_original)*rur
        end
    ) as wapi_without_discount_sum_in_rubles,                           -- Сумма пополнения баланса WABA без скидки партнера в рублях
    (case when balance_and_payments_with_balance_spent.currency='RUR' then balance_spent_on_subscription_original
    else balance_spent_on_subscription_original*rur
    end) as balance_spent_on_subscription_sum_in_rubles,                -- Сумма бонусов потраченных на подписку в рублях
    (case when balance_and_payments_with_balance_spent.currency='RUR' then balance_spent_on_waba_original
    else balance_spent_on_waba_original*rur
    end) as balance_spent_on_waba_sum_in_rubles,                        -- Сумма бонусов потраченных на баланс WABA в рублях
    round((case when balance_and_payments_with_balance_spent.currency='RUR' then original_sum_to_pay
    else original_sum_to_pay*rur
    end),2) as sum_in_rubles_to_pay,                                    -- Сумма, которую нужно заплатить для оплаты подписки в рублях
    (case when balance_and_payments_with_balance_spent.currency='RUR' then subscription_sum_without_balance_spent_by_client_original
    else subscription_sum_without_balance_spent_by_client_original*rur
    end) as subscription_sum_without_balance_spent_by_client_in_rubles  -- Стоимость подписки без бонусов клиента в рублях
 from balance_and_payments_with_balance_spent 
 left join
           `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted
            on exchange_rates_unpivoted._ibk = balance_and_payments_with_balance_spent.paid_date
            and exchange_rates_unpivoted.currency = balance_and_payments_with_balance_spent.currency),

balance_and_payments_with_good_balance_spent as (
    select *, 
    (
        case when good_balance_spent>=balance_spent_on_subscription_sum_in_rubles then  balance_spent_on_subscription_sum_in_rubles
        else good_balance_spent
    end
    ) as good_balance_spent_on_subscription,                -- Сумма потраченных хороших бонусов на подписку в рублях
    good_balance_spent-(case when wapi_original_sum=0 then good_balance_spent
    when good_balance_spent>=balance_spent_on_subscription_sum_in_rubles then  balance_spent_on_subscription_sum_in_rubles
    else good_balance_spent
    end
    ) as good_balance_spent_on_waba_in_rubles,              -- Сумма потраченных хороших бонусов на WABA баланс в рублях
    (
        case when subscription_sum_without_balance_spent_by_client_in_rubles>=card_or_bills_sum_in_rubles then card_or_bills_sum_in_rubles
        else subscription_sum_without_balance_spent_by_client_in_rubles
        end
        ) as card_or_bills_spent_on_subscription_in_rubles, -- Сумма потраченных реальных денег на подписку в рублях
    card_or_bills_sum_in_rubles-(
        case when wapi_original_sum=0 then card_or_bills_sum_in_rubles
        when subscription_sum_without_balance_spent_by_client_in_rubles>=card_or_bills_sum_in_rubles then card_or_bills_sum_in_rubles
        else subscription_sum_without_balance_spent_by_client_in_rubles
        end
    ) as card_or_bills_spent_on_waba_balance_in_rubles      -- Сумма потраченных реальных денег на баланс WABA в рублях
  from balance_and_payments_with_converted_sums ),

balance_and_payments_with_good_and_bad_balance_spent as (
    select *,
    round(balance_spent_on_subscription_sum_in_rubles,2)-round(good_balance_spent_on_subscription,2) as bad_balance_spent_on_subscription_sum_in_rubles,    -- Сумма потраченных плохих бонусов на подписку в рублях
    round(balance_spent_on_waba_sum_in_rubles,2) - round(good_balance_spent_on_waba_in_rubles,2) as bad_balance_spent_on_waba_balance_sum_in_rubles         -- Сумма потраченных плохих бонусов на WABA баланс в рублях
    from balance_and_payments_with_good_balance_spent
)
    -- Таблица платежей, которая отражает количество реально потраченных денег, хороших бонусов и плохих бонусов на различные операции Wazzup
select *,
good_balance_spent_on_subscription+card_or_bills_spent_on_subscription_in_rubles as sum_in_rubles_spent_on_subscription,    -- Сумма оплаты подписки в рублях
good_balance_spent_on_waba_in_rubles+card_or_bills_spent_on_waba_balance_in_rubles as sum_in_rubles_spent_on_waba_balance   -- Сумма оплаты WABA баланса в рублях
from balance_and_payments_with_good_and_bad_balance_spent