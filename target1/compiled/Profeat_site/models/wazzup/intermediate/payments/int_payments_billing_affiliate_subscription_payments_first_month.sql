with
    billing_affiliate as (
        select
            billingaffiliate.account_id,
            billingaffiliate.subscription_owner,
            subscriptionupdates.paid_at_billing_date as occured_date,
            subscriptionupdates.guid as subscriptionupdates_guid,
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
            ) as has_partner_paid

        from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` billingaffiliate
        left join
            `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted
            on exchange_rates_unpivoted._ibk = billingaffiliate.occured_date
            and exchange_rates_unpivoted.currency = billingaffiliate.currency
        inner join
            `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency_billing_date` subscriptionupdates
            on subscriptionupdates.guid = billingaffiliate.subscription_update_id
        left join
            `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_card` payments
            on billingaffiliate.payment_guid = payments.guid
        where object = 'subscription'
    ),

    billing_affiliate_to_deduplicate as (
        select
            *,
            row_number() over (
                partition by subscriptionupdates_guid order by has_partner_paid desc
            ) as rn
        from billing_affiliate
    ),

    billing_affiliated_deduplicated_and_aggregated_first_month as (
        select
            billing_affiliate_to_deduplicate.subscription_owner as account_id,
            has_partner_paid,
            occured_date,
            sum(sum_in_rubles) as sum_in_rubles,
            sum(balance_to_withdraw_in_rubles) as balance_to_withdraw_in_rubles
        from billing_affiliate_to_deduplicate
        inner join
            `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts
            on accounts.account_id = billing_affiliate_to_deduplicate.subscription_owner
        where
            billing_affiliate_to_deduplicate.occured_date
            <= date_add(accounts.register_date, interval 1 month)
            and accounts.type = 'standart'
            and rn = 1
        group by 1, 2, 3
    ),
    billing_affiliate_with_invalid_bills as (
        select
            billing_affiliated_deduplicated_and_aggregated_first_month.*,
            sum_in_rubles_invalid_bills,
            paid_date
        from billing_affiliated_deduplicated_and_aggregated_first_month
        left join
            `dwh-wazzup`.`dbt_nbespalov`.`int_payments_bills_only_invalid_first_month` bills
            on bills.account_id
            = billing_affiliated_deduplicated_and_aggregated_first_month.account_id
            and bills.paid_date
            <= billing_affiliated_deduplicated_and_aggregated_first_month.occured_date
    ),
    balance_sum_in_rubles as (
        select account_id, occured_date, min(sum_in_rubles) as sum_in_rubles
        from billing_affiliate_with_invalid_bills
        where has_partner_paid = 2
        group by 1, 2
    ),

    invalid_bills_sum_in_rubles as (
        select
            account_id,
            paid_date,
            min(sum_in_rubles_invalid_bills) as sum_in_rubles_invalid_bills
        from billing_affiliate_with_invalid_bills
        where has_partner_paid = 2
        group by 1, 2
    ),

    balance_sum_in_rubles_aggregated_by_account_id as (
        select account_id, 
        sum(sum_in_rubles) as sum_in_rubles
        from balance_sum_in_rubles
        group by 1
    ),

    invalid_bills_sum_in_rubles_aggregated_by_account_id as (
        select
            account_id, 
            sum(sum_in_rubles_invalid_bills) as sum_in_rubles_invalid_bills
        from invalid_bills_sum_in_rubles
        group by 1
    ),

    bills_without_sum_in_rubles as (
        select
            balance_sum_in_rubles_aggregated_by_account_id.*,
            sum_in_rubles_invalid_bills,
            (
                case
                    when sum_in_rubles <= sum_in_rubles_invalid_bills
                    then sum_in_rubles
                    when sum_in_rubles > sum_in_rubles_invalid_bills
                    then sum_in_rubles_invalid_bills
                    else 0
                end
            ) as sum_in_rubles_without_invalid_bills
        from balance_sum_in_rubles_aggregated_by_account_id
        left join
            invalid_bills_sum_in_rubles_aggregated_by_account_id
            on invalid_bills_sum_in_rubles_aggregated_by_account_id.account_id
            = balance_sum_in_rubles_aggregated_by_account_id.account_id
    ),

 billing_without_sum_in_rubles as (   

select
    account_id,
    sum(
        (
            case
                when sum_in_rubles_invalid_bills is not null
                then sum_in_rubles + balance_to_withdraw_in_rubles
                else sum_in_rubles
            end
        )
    ) as sum_in_rubles_without_invalid_bills
from billing_affiliate_with_invalid_bills
where has_partner_paid != 2
group by 1
union all
select account_id, 
sum_in_rubles_without_invalid_bills
from bills_without_sum_in_rubles),

bills_aggregated as (
select account_id,                                                              -- ID аккаунта
sum(sum_in_rubles_without_invalid_bills) as sum_in_rubles_without_invalid_bills -- Сумма оплаты в рублях без невалидных счетов
from billing_without_sum_in_rubles
group by 1)
    -- Таблица сумм платежей по аккаунтам, совершенных в первый месяц после регистрации
select * from bills_aggregated