with
    billing_affiliate_data_with_transaction_type as (
        select
            billingaffiliate.sum as sum_in_rubles,
            occured_at,
            (
                case
                    when object in ('payment','transfer') and billingaffiliate.original_sum>0 --оставляем только пополнения счета (origian_sum<0, если произошел перевод другому аккаунту)
                    then 'good_balance'
                    when object = 'subscription' and subscriptionupdates.guid is not null
                    then 'subscription'
                    when billingaffiliate.sum >= 0
                    then 'bad_balance'
                end
            ) as transaction_type,
            subscription_update_id,
            object,
            billingaffiliate.account_id
        from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` billingaffiliate
        left join `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency_billing_date` subscriptionupdates
        on billingaffiliate.subscription_update_id=subscriptionupdates.guid
    ),

    billing_with_good_balance_function as (
        select
            account_id,
            dbt_nbespalov.good_balance(
                array_agg(sum_in_rubles order by occured_at asc),
                array_agg(transaction_type order by occured_at asc),
                array_agg(subscription_update_id order by occured_at asc)
            ) good_balance_data
        from billing_affiliate_data_with_transaction_type
        group by 1
    ),

    billing_with_good_balance_function_data as (

        select                  -- Таблица, которая показывает уникальные платежи: сколько бонусов потратили на подписку 
            account_id,                                                                             -- ID аккаунта
            good_balance.subscription_update_id,                                                    -- Индентификатор изменения. Соответствует полю guid из таблицы subscriptionUpdates 
            good_balance.good_balance_spent                                                         -- Сумма используемых хороших ("Счет недействителен", перевод денег с другого аккаунта, пополнение партнерского счета) бонусов
        from billing_with_good_balance_function                                                 
        cross join unnest(billing_with_good_balance_function.good_balance_data) good_balance
    )

select *
from billing_with_good_balance_function_data