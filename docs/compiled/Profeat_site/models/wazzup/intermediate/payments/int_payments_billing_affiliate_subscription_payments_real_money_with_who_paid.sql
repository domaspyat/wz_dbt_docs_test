/*
Модель нужна для определения источника платежа бонусов


*/
WITH real_money_aggregated AS ( --Информация по сумме потраченных хороших бонусов в рамках account_id/subscription_update_id
                              SELECT account_id
                                   , subscription_update_id
                                   , sum(good_balance_spent) AS good_balance_spent
                              FROM `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_track_real_money_spending`
                              GROUP BY 1, 2
                              ),
     billing_affiliate AS ( --Определяем источник списания с бонусного/партнерского счета
/*

has_partner_paid может принимать 3 значения: 0,1,2
0 - списание с бонусного счета клиента. У пользователя (НЕ ПАРТНЕРА) есть партнер. Партнер оплатил подписку, используя бонусы клиента. Партнер НЕ может оплатить подписку целиком, используя бонусы клиента. Какую-то часть он ОБЯЗАН оплатить со своего счета. Здесь укажется сколько бонусов потратил клиент на данную подписку.
1 - списание с партнерского счета. У пользователя есть партнер, который заплатил за дочку (дочки, за которых платит партнер). Здесь укажется сколько потратил партнер на данную подписку.
2 - клиент или партнер заплатил за себя. указывается БОНУСНАЯ часть. Например, клиент заплатил за подписку 3000, из которых 1900 - бонусы, 1100 - реальные деньги. Сюда попадает 1900. Часть с реальными деньгами добавляется в другой модели (int_payments_billing_affiliate_subscription_payments_real_money_bills_and_payments_with_balance)

*/

                              SELECT billingaffiliate.account_id            -- ID аккаунта
                                   , billingaffiliate.subscription_owner    -- ID аккаунта владельца подписки
                                   , subscriptionupdates.paid_at_billing_date AS occured_date           -- Дата события
                                   , subscriptionupdates.paid_at_billing      AS occured_at             -- Дата и время события (04.03.2025)
                                   , subscriptionupdates.guid                 AS subscription_update_id -- ID изменения, соответствует guid из subscriptionUpdates
                                   , subscriptionupdates.subscription_id    -- ID подписки
                                   , action -- Действие с подпиской
                                   , (
                                  CASE
                                      WHEN billingaffiliate.currency = 'RUR'
                                          THEN billingaffiliate.sum
                                      WHEN rur IS NOT NULL
                                          THEN (billingaffiliate.sum) * rur
                                          --Ниже легаси, отрабатывало раньше, когда в таблице не было данных за некоторые периоды
                                          
                                      WHEN billingaffiliate.currency = 'EUR' AND rur IS NULL
                                          THEN (billingaffiliate.sum) * 85
                                      WHEN billingaffiliate.currency = 'USD' AND rur IS NULL
                                          THEN (billingaffiliate.sum) * 75
                                      WHEN billingaffiliate.currency = 'KZT' AND rur IS NULL
                                          THEN (billingaffiliate.sum) * 0.24
                                      END
                                  )                                           AS sum_in_rubles                      -- Сумма, потраченная на действие в рублях
                                   , subscriptionupdates.sum_in_rubles        AS sum_in_rubles_full_subscription    -- Сумма, потраченная на действие без скидок
                                   , subscriptionupdates.sum                  AS subscriptionupdates_original_sum   -- Сумма, потраченная на действие из subscriptionUpdates
                                   , subscriptionupdates.wapi_original_sum    AS wapi_original_sum                  -- Сумма, потраченная на пополнение баланса WABA
                                   , billingaffiliate.sum                     AS billing_affiliate_original_sum     -- Сумма, потраченная на действие из billingAffiliate
                                   , billingaffiliate.currency                AS billingaffiliate_currency          -- Валюта из billingAffiliate
                                   , subscriptionupdates.currency             AS subscription_updates_currency      -- Валюта из subscriptionUpdates
                                   , (
                                  CASE
                                      WHEN
                                          payments.sum = 0 -- В табличке payments account_id  = аккаунт владельца подписки, 
                                                  AND payments.account_id = billingaffiliate.account_id
                                                  AND abs(billingaffiliate.sum) != subscriptionupdates.sum --условие ничего не меняет
                                                  AND account_type NOT IN ('partner', 'tech-partner')
                                                  AND payments.partner_account_id IS NOT NULL
                                                                                             THEN 0
                                      WHEN
                                          payments.sum = 0 --условие ничего не меняет
                                                  AND payments.account_id != billingaffiliate.account_id
                                                  AND abs(billingaffiliate.sum) != subscriptionupdates.sum --условие ничего не меняет
                                                                                             THEN 1
                                      WHEN billingaffiliate.account_id != subscription_owner THEN 1
                                      ELSE 2
                                      END
                                  )                                           AS has_partner_paid   -- Метрика, которая определяет кто и как платил. Описана выше
                                   , (
                                  CASE
                                      WHEN billingaffiliate.currency = 'RUR'
                                          THEN real_money_aggregated.good_balance_spent
                                      WHEN rur IS NOT NULL
                                          THEN (real_money_aggregated.good_balance_spent) * rur
                                      WHEN billingaffiliate.currency = 'EUR' AND rur IS NULL
                                          THEN (real_money_aggregated.good_balance_spent) * 85
                                      WHEN billingaffiliate.currency = 'USD' AND rur IS NULL
                                          THEN (real_money_aggregated.good_balance_spent) * 75
                                      WHEN billingaffiliate.currency = 'KZT' AND rur IS NULL
                                          THEN (real_money_aggregated.good_balance_spent) * 0.24
                                      END
                                  )                                           AS good_balance_spent     -- Сумма хороших бонусов, потраченная на действие
                                   , real_money_aggregated.account_id         AS real_money_account_id  --TODO дропнуть поле, оно равно billingAffiliate.accountId
                                   , wapi_transactions_in_rubles    -- Сумма, потраченная на пополнение баланса WABA в рублях
                                   , partner_discount               -- Скидка партнера
                                   , start_date                     -- Дата начала подписки
                                   , account_type                   -- Тип аккаунта

                              FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate`  billingaffiliate
        LEFT JOIN
            `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted
                              ON exchange_rates_unpivoted._ibk = billingaffiliate.occured_date
                                  AND exchange_rates_unpivoted.currency = billingaffiliate.currency
                                  INNER JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency_billing_date` subscriptionupdates
                                                            ON subscriptionupdates.guid = billingaffiliate.subscription_update_id
                                  
                                  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_card` payments ON subscriptionupdates.activation_reason_id = payments.guid
                                  
                                  LEFT JOIN real_money_aggregated ON real_money_aggregated.subscription_update_id=subscriptionupdates.guid
                                                                    AND real_money_aggregated.account_id=billingaffiliate.account_id
                                  INNER JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` account_type_data
                                                ON billingaffiliate.account_id=account_type_data.account_id AND billingaffiliate.occured_date>=account_type_data.start_date AND billingaffiliate.occured_date<=account_type_data.end_date
                              WHERE OBJECT = 'subscription'
                                AND NOT EXISTS (SELECT invoice_id FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` invalid WHERE invalid.invoice_id=billingaffiliate.invoice_id
                                AND OBJECT ='refundForInvoice')
                              ),


     billing_affiliate_to_deduplicate AS (
                              SELECT *
                                   , row_number() OVER (PARTITION BY subscription_update_id, has_partner_paid ORDER BY start_date DESC) AS rn
                              FROM billing_affiliate
                              ),

     billing_affiliate_deduplicated AS (
                              SELECT *
                              from  billing_affiliate_to_deduplicate
                              WHERE rn = 1
                              )
                              -- Таблица платежей, которая показывает кто и сколько платил реальных денег за подписки
SELECT * --TODO убрать rn
FROM billing_affiliate_deduplicated