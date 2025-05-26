WITH billingaffiliate AS (
                         SELECT sum
                              , occured_at
                              , object
                              , coalesce(subscription_update_id,
                                         CASE WHEN (object = 'payment' AND method = 'transfer' AND original_sum < 0)
                                              OR   (object = 'convertation' AND original_sum < 0)
                                              OR object = 'withdrawal'
                                              THEN CAST(ba_id AS string) 
                                         END) AS subscription_update_id
                              , subscription_update_id as subscription_update_id_for_condition
                              , method
                              , account_id
                              , original_sum
                              , guid as guid_
                         FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate`
                         )
                         

     ,billing_affiliate_data_with_transaction_type AS ( 
                         SELECT billingaffiliate.sum AS sum_in_rubles
                              , occured_at
                              , CASE --release_transfer при переезде со старого биллинга на новый, баланс который находился в старой таблице(billing), переезжал в номую(billingAffiliate) c таким обжектом.
                                    WHEN object IN ('payment', 'release_transfer', 'transfer') AND billingaffiliate.original_sum >= 0 
                                    THEN 'good_balance' --оставляем только пополнения счета (origian_sum<0, если произошел перевод другому аккаунту). Сюда также попадают невалидные платежи (с некорректно сформированной платежкой со стороны клиента, например, неверно указанное назначение платежа). В таких случаях object = 'payment', provider = '1C', details->>'invalid' = 'true'
                                    
                                    WHEN object = 'subscription' --списание денег на подписку (sub_upd.guid в последний раз был null c object = 'subscription' в сентябре 2021, либо это кейсы, когда подписку удаляют, следовательно subscription_update_state = 'deleted')
                                        OR (object = 'payment' AND method = 'transfer' AND billingaffiliate.original_sum < 0) --была отправка денег другому аккаунту. В таком случае мы списываем бонусы, которые у него были.
                                        OR (object = 'convertation' AND billingaffiliate.original_sum < 0)
                                        OR object = 'withdrawal'
                                        OR object = 'refund'
                                        OR object = 'takeAway'
                                        OR (object = 'transfer' AND billingaffiliate.original_sum < 0)
                                    THEN 'subscription'
                                    
                                    WHEN billingaffiliate.sum >= 0 --ситуаций, когда sum < 0  - нет, условие как будто бы не нужно
                                    THEN 'bad_balance'
                                 END    AS transaction_type
                              , subscription_update_id -- нужно для выделения времени транзакций. В случаях с переводом денег subscription_update_id не пишется, следовательно признака для соединения таблиц не будет.
                              , object
                              , billingaffiliate.account_id
                         FROM billingaffiliate
                             LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency_billing_date` subscriptionupdates
                         ON billingaffiliate.subscription_update_id=subscriptionupdates.guid
                         --Дописали условие по учету кейсов с непримененными изменениями (state != active в sub_updates). Евсли в биллинг аффилиэйт есть sub_upd_id, а в sub_updates нет, это означает, что изменение НЕ БЫЛО применено (state != 'activated'). Такие кейсы исключаем
                         WHERE billingaffiliate.subscription_update_id_for_condition IS NULL
                         or NOT(billingaffiliate.subscription_update_id_for_condition IS NOT NULL and subscriptionUpdates.guid is null)
                         
                         ),
     billing_with_good_balance_function AS (
                         SELECT account_id
                              , dbt_nbespalov.good_balance(
                                array_agg(sum_in_rubles ORDER BY occured_at ASC)
                              , array_agg(transaction_type ORDER BY occured_at ASC)
                              , array_agg(subscription_update_id ORDER BY occured_at ASC)
                             ) good_balance_data
                         FROM billing_affiliate_data_with_transaction_type
                         GROUP BY 1
                         ),
     billing_with_good_balance_function_data AS (-- Таблица, которая показывает сколько бонусов потратили на подписку
                         SELECT account_id                          -- ID аккаунта
                              , good_balance.subscription_update_id -- Индентификатор изменения. Соответствует полю guid из таблицы subscriptionUpdates
                              , good_balance.good_balance_spent     -- Сумма используемых хороших ("Счет недействителен", перевод денег с другого аккаунта, пополнение партнерского счета) бонусов
                         FROM billing_with_good_balance_function
                             CROSS JOIN unnest(billing_with_good_balance_function.good_balance_data) good_balance
                         )

SELECT  billing_with_good_balance_function_data.*except(subscription_update_id)
      , CASE WHEN OBJECT = 'payment' AND METHOD = 'transfer' AND original_sum < 0
             THEN NULL
        ELSE subscription_update_id
        END AS subscription_update_id
      , occured_at AS good_balance_spent_datetime
FROM billing_with_good_balance_function_data
LEFT JOIN billingaffiliate USING(subscription_update_id,account_id)
/* TODO нужны ли строки с null sub_update_id */