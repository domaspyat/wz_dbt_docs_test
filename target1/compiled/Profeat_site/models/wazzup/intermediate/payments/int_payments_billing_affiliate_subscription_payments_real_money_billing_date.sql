with real_money_aggregated as (
    select account_id,
    subscription_update_id,
    sum(good_balance_spent) as good_balance_spent 
    from  `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_track_real_money_spending`
    group by 1,2
),
billing_affiliate as (
        select
            billingaffiliate.account_id,
            billingaffiliate.subscription_owner,
            paid_at_billing_completed_date as occured_date,
            subscriptionupdates.guid as subscription_update_id,
            subscriptionupdates.subscription_id,
            action,
            (case when subscriptionupdates.currency=billingaffiliate.currency then True else False end) as is_subscription_currency_the_same_as_billing_affilate, --нужно для обработки случаев, когда у клиента валюта отличается от валюты партнера. Пример: в случае если у партнера ЛК в рублях, в subscriptionUpdates стоимость подписки будет 6000, а партнер заплатит в KZT 6300 рублей в зависимости от курса. А у нас многое завязано на подсчет стоимости подписки
                        (
                case
                    when billingaffiliate.currency = 'RUR'
                    then billingaffiliate.sum
                    when rur is not null
                    then (billingaffiliate.sum) * rur
                    when billingaffiliate.currency = 'EUR' and rur is null
                    then (billingaffiliate.sum) * 85
                    when billingaffiliate.currency = 'USD' and rur is null
                    then (billingaffiliate.sum) * 75
                    when billingaffiliate.currency = 'KZT' and rur is null
                    then (billingaffiliate.sum) * 0.24
                end
            ) as sum_in_rubles,
            (
                case
                    when billingaffiliate.currency = 'RUR'
                    then billingaffiliate.sum - coalesce(abs(balance_to_withdraw), 0)
                    when rur is not null
                    then
                        (billingaffiliate.sum - coalesce(abs(balance_to_withdraw), 0))
                        * rur
                    when billingaffiliate.currency = 'EUR' and rur is null
                    then (billingaffiliate.sum - coalesce(balance_to_withdraw, 0)) * 85
                    when billingaffiliate.currency = 'USD' and rur is null
                    then (billingaffiliate.sum - coalesce(balance_to_withdraw, 0)) * 75
                    when billingaffiliate.currency = 'KZT' and rur is null
                    then
                        (billingaffiliate.sum - coalesce(balance_to_withdraw, 0)) * 0.24
                end
            ) as sum_in_rubles_without_balance,

            (
                case
                    when billingaffiliate.currency = 'RUR'
                    then coalesce(abs(balance_to_withdraw), 0)
                    when rur is not null
                    then coalesce(abs(balance_to_withdraw), 0) * rur
                    when billingaffiliate.currency = 'EUR' and rur is null
                    then coalesce(abs(balance_to_withdraw), 0) * 85
                    when billingaffiliate.currency = 'USD' and rur is null
                    then coalesce(abs(balance_to_withdraw), 0) * 75
                    when billingaffiliate.currency = 'KZT' and rur is null
                    then coalesce(abs(balance_to_withdraw), 0) * 0.24
                end
            ) as balance_to_withdraw_in_rubles,
            coalesce(balance_to_withdraw, 0) as balance_to_withdraw,
            subscriptionupdates.sum_in_rubles as sum_in_rubles_full_subscription,
            (
                case
                    when
                        payments.sum = 0
                        and payments.account_id = billingaffiliate.account_id
                        and abs(billingaffiliate.sum) != subscriptionupdates.sum
                        and balance_to_withdraw != 0
                    then 0
                    when
                        payments.sum = 0
                        and payments.account_id != billingaffiliate.account_id
                        and abs(billingaffiliate.sum) != subscriptionupdates.sum
                        and balance_to_withdraw != 0
                    then 1
                    when billingaffiliate.account_id!=subscription_owner then 1
                    else 2
                end
            ) as has_partner_paid,
         (
                case
                    when billingaffiliate.currency = 'RUR'
                    then real_money_aggregated.good_balance_spent
                    when rur is not null
                    then (real_money_aggregated.good_balance_spent) * rur
                    when billingaffiliate.currency = 'EUR' and rur is null
                    then (real_money_aggregated.good_balance_spent) * 85
                    when billingaffiliate.currency = 'USD' and rur is null
                    then (real_money_aggregated.good_balance_spent) * 75
                    when billingaffiliate.currency = 'KZT' and rur is null
                    then (real_money_aggregated.good_balance_spent) * 0.24
                end
            ) as good_balance_spent,

        real_money_aggregated.account_id as real_money_account_id,
        wapi_transactions_in_rubles,
        partner_discount,
        start_date,
        account_type

        from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate`  billingaffiliate
        left join
            `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted
            on exchange_rates_unpivoted._ibk = billingaffiliate.occured_date
            and exchange_rates_unpivoted.currency = billingaffiliate.currency
        inner join
            `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency_billing_date`  subscriptionupdates
            on subscriptionupdates.guid = billingaffiliate.subscription_update_id
        left join
           `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_card` payments
            on billingaffiliate.payment_guid = payments.guid
        left join real_money_aggregated 
        on real_money_aggregated.subscription_update_id=subscriptionupdates.guid
        and real_money_aggregated.account_id=billingaffiliate.account_id
        inner join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` account_type_data 
        on billingaffiliate.account_id=account_type_data.account_id and  billingaffiliate.occured_date>=account_type_data.start_date and billingaffiliate.occured_date<=account_type_data.end_date
        where object = 'subscription' and not exists (select invoice_id from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` invalid where invalid.invoice_id=billingaffiliate.invoice_id and object='refundForInvoice') ),


billing_affiliate_to_deduplicate as (

    select *, row_number() over (partition by subscription_update_id, has_partner_paid order by start_date desc) rn from billing_affiliate
  ) ,  

billing_affiliate_deduplicated as (
select *, 
(case when partner_discount is not null and ((occured_date>='2022-11-28'  and account_type='partner') or (occured_date>='2023-02-10' and account_type='tech-partner')) then 0.1*wapi_transactions_in_rubles end) as wapi_discount_for_partners
from billing_affiliate_to_deduplicate where rn=1),


balance_spending_partner_and_client as (
select subscription_owner as account_id, 
occured_date, 
subscription_id,  
subscription_update_id,
action,
max(partner_discount) as partner_discount,
sum(good_balance_spent) as good_balance_spent, 
max(wapi_transactions_in_rubles) as wapi_transactions_in_rubles,
max(sum_in_rubles_full_subscription) as sum_in_rubles_full_subscription,
max(sum_in_rubles_full_subscription-wapi_transactions_in_rubles) as subscription_sum,
max(wapi_discount_for_partners)  as wapi_discount_for_partners,
max(is_subscription_currency_the_same_as_billing_affilate) as is_subscription_currency_the_same_as_billing_affilate
from billing_affiliate_deduplicated        
where (has_partner_paid=1 or (has_partner_paid=0 and subscription_owner=real_money_account_id))
group by 1,2,3,4,5
),

--этот подзапрос нужен в том случае, если пользователь оплачивает подписку переводом и хорошими бонусами (около 4 оплат)
balance_spending_standart as (
select subscription_owner as account_id, 
occured_date, 
subscription_id,  
subscription_update_id,
action,
max(partner_discount) as partner_discount,
max(good_balance_spent) as good_balance_spent, 
max(wapi_transactions_in_rubles) as wapi_transactions_in_rubles,
max(sum_in_rubles_full_subscription) as sum_in_rubles_full_subscription,
max(sum_in_rubles_full_subscription-wapi_transactions_in_rubles) as subscription_sum  ,
max(wapi_discount_for_partners)  as wapi_discount_for_partners,
max(is_subscription_currency_the_same_as_billing_affilate) as is_subscription_currency_the_same_as_billing_affilate
from billing_affiliate_deduplicated        
where has_partner_paid=2
group by 1,2,3,4,5),

all_balance_spending as (
    select * from balance_spending_partner_and_client
    union all 
    select * from balance_spending_standart
),


good_balance_spent_aggregated as (
select account_id,          -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
occured_date,               -- Дата изменения
subscription_id,            -- ID подписки
action,                     -- Какое изменение оплачено?
subscription_update_id,     -- ID изменения, соответствует guid из subscriptionUpdates
max(partner_discount) as partner_discount,                                  -- Скидка партнера
sum(subscription_sum) as subscription_sum_only,                             -- Сумма оплаты подписки
sum(wapi_transactions_in_rubles) as wapi_transactions_in_rubles,            -- Сумма пополнения баланса WABA в рублях
sum(sum_in_rubles_full_subscription) as sum_in_rubles_full_subscription,    -- Сумма оплаты подписки в рублях вместе с балансом WABA
sum(good_balance_spent) as good_balance_spent,                              -- Сумма потраченных хороших бонусов
max(wapi_discount_for_partners)  as wapi_discount_for_partners,             -- Комиссия партнера за пополнение баланса WABA
max(is_subscription_currency_the_same_as_billing_affilate) as is_subscription_currency_the_same_as_billing_affilate -- Валюта подписки такая же, как в billingAffiliate?
 from all_balance_spending
 group by 1,2,3,4,5
),

good_balance_aggregated as (

select *,
 (1-coalesce(partner_discount,0)) * subscription_sum_only as subscription_sum
 from good_balance_spent_aggregated)
    -- Таблица платежей по биллингу с суммой реально потраченных денег
select * from good_balance_aggregated