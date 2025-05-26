WITH real_money_aggregated AS (
                              SELECT account_id
                                   , subscription_update_id
                                   , sum(good_balance_spent) AS good_balance_spent
                              FROM `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_track_real_money_spending`
                              GROUP BY 1, 2
                              ),
     billing_affiliate AS (
                              SELECT billingaffiliate.account_id
                                   , billingaffiliate.subscription_owner
                                   , subscriptionupdates.paid_at_billing_date AS occured_date
                                   , paid_at_billing_completed_date           AS occured_date_change_in_product
                                   , subscriptionupdates.guid                 AS subscription_update_id
                                   , subscriptionupdates.subscription_id
                                   , action
                                   , (CASE WHEN subscriptionupdates.currency = billingaffiliate.currency THEN TRUE ELSE FALSE END)                    AS is_subscription_currency_the_same_as_billing_affilate --нужно для обработки случаев, когда у клиента валюта отличается от валюты партнера. Пример: в случае если у партнера ЛК в рублях, в subscriptionUpdates стоимость подписки будет 6000, а партнер заплатит в KZT 6300 рублей в зависимости от курса. А у нас многое завязано на подсчет стоимости подписки
                                   , (CASE WHEN billingaffiliate.currency = 'RUR' THEN billingaffiliate.sum
                                           WHEN rur IS NOT NULL                   THEN (billingaffiliate.sum) * rur
                                           WHEN billingaffiliate.currency = 'EUR' AND rur IS NULL THEN (billingaffiliate.sum) * 85
                                           WHEN billingaffiliate.currency = 'USD' AND rur IS NULL THEN (billingaffiliate.sum) * 75
                                           WHEN billingaffiliate.currency = 'KZT' AND rur IS NULL THEN (billingaffiliate.sum) * 0.24
                                      END
                                  )                                           AS sum_in_rubles
                                   , (CASE WHEN billingaffiliate.currency = 'RUR' THEN billingaffiliate.sum - coalesce(abs(balance_to_withdraw), 0)
                                           WHEN rur IS NOT NULL                   THEN (billingaffiliate.sum - coalesce(abs(balance_to_withdraw), 0)) * rur
                                           WHEN billingaffiliate.currency = 'EUR' AND rur IS NULL THEN (billingaffiliate.sum - coalesce(balance_to_withdraw, 0)) * 85
                                           WHEN billingaffiliate.currency = 'USD' AND rur IS NULL THEN (billingaffiliate.sum - coalesce(balance_to_withdraw, 0)) * 75
                                           WHEN billingaffiliate.currency = 'KZT' AND rur IS NULL THEN (billingaffiliate.sum - coalesce(balance_to_withdraw, 0)) * 0.24
                                      END
                                  )                                           AS sum_in_rubles_without_balance
                                   , CASE WHEN billingaffiliate.currency = 'RUR' THEN coalesce(abs(balance_to_withdraw), 0)
                                           WHEN rur IS NOT NULL                   THEN coalesce(abs(balance_to_withdraw), 0) * rur
                                           WHEN billingaffiliate.currency = 'EUR' AND rur IS NULL THEN coalesce(abs(balance_to_withdraw), 0) * 85
                                           WHEN billingaffiliate.currency = 'USD' AND rur IS NULL THEN coalesce(abs(balance_to_withdraw), 0) * 75
                                           WHEN billingaffiliate.currency = 'KZT' AND rur IS NULL THEN coalesce(abs(balance_to_withdraw), 0) * 0.24
                                     END
                                                                              AS balance_to_withdraw_in_rubles
                                   , coalesce(balance_to_withdraw, 0)         AS balance_to_withdraw
                                   , subscriptionupdates.sum_in_rubles        AS sum_in_rubles_full_subscription
                                   , CASE WHEN payments.sum = 0
                                                  AND payments.account_id = billingaffiliate.account_id
                                                  AND abs(billingaffiliate.sum) != subscriptionupdates.sum
                                                  AND balance_to_withdraw != 0
                                                                                             THEN 0
                                          WHEN payments.sum = 0
                                                  AND payments.account_id != billingaffiliate.account_id
                                                  AND abs(billingaffiliate.sum) != subscriptionupdates.sum
                                                  AND balance_to_withdraw != 0
                                                                                             THEN 1
                                          WHEN billingaffiliate.account_id != subscription_owner THEN 1
                                      ELSE 2
                                      END
                                                                              AS has_partner_paid
                                   , CASE WHEN billingaffiliate.currency = 'RUR' THEN real_money_aggregated.good_balance_spent
                                          WHEN rur IS NOT NULL                   THEN (real_money_aggregated.good_balance_spent) * rur
                                          WHEN billingaffiliate.currency = 'EUR' AND rur IS NULL THEN (real_money_aggregated.good_balance_spent) * 85
                                          WHEN billingaffiliate.currency = 'USD' AND rur IS NULL THEN (real_money_aggregated.good_balance_spent) * 75
                                          WHEN billingaffiliate.currency = 'KZT' AND rur IS NULL THEN (real_money_aggregated.good_balance_spent) * 0.24
                                      END
                                                                              AS good_balance_spent
                                   , real_money_aggregated.account_id         AS real_money_account_id
                                   , wapi_transactions_in_rubles
                                   , partner_discount
                                   , start_date
                                   , account_type
                              FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` billingaffiliate
                                  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted ON exchange_rates_unpivoted._ibk = billingaffiliate.occured_date
                                                                                                                       AND exchange_rates_unpivoted.currency = billingaffiliate.currency
                                  INNER JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency_billing_date` subscriptionupdates ON subscriptionupdates.guid = billingaffiliate.subscription_update_id
                                  LEFT JOIN  `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_card` payments ON billingaffiliate.payment_guid = payments.guid
                                  LEFT JOIN real_money_aggregated ON real_money_aggregated.subscription_update_id=subscriptionupdates.guid
                                                                AND real_money_aggregated.account_id=billingaffiliate.account_id
                                  INNER JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` account_type_data ON billingaffiliate.account_id=account_type_data.account_id AND billingaffiliate.occured_date>=account_type_data.start_date AND billingaffiliate.occured_date<=account_type_data.end_date
                              WHERE object = 'subscription'
                                AND NOT EXISTS (SELECT invoice_id FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` invalid WHERE invalid.invoice_id=billingaffiliate.invoice_id
                                AND object='refundForInvoice')
     ),
     billing_affiliate_to_deduplicate AS (
                              SELECT *
                                   , row_number() OVER (PARTITION BY subscription_update_id, has_partner_paid ORDER BY start_date DESC) AS rn
                              FROM billing_affiliate
                              ),
     billing_affiliate_deduplicated AS (
                              SELECT *
                                   , CASE WHEN partner_discount IS NOT NULL
                                      AND ((occured_date >= '2022-11-28' AND account_type = 'partner')
                                          OR (occured_date >= '2023-02-10' AND account_type = 'tech-partner'))
                                               THEN 0.1 * wapi_transactions_in_rubles END AS wapi_discount_for_partners
                              FROM billing_affiliate_to_deduplicate
                              WHERE rn = 1
                              ),
     balance_spending_partner_and_client AS (
                              SELECT subscription_owner                                                 AS account_id
                                   , occured_date
                                   , subscription_id
                                   , subscription_update_id
                                   , action
                                   , max(partner_discount)                                              AS partner_discount
                                   , sum(good_balance_spent)                                            AS good_balance_spent
                                   , max(wapi_transactions_in_rubles)                                   AS wapi_transactions_in_rubles
                                   , max(sum_in_rubles_full_subscription)                               AS sum_in_rubles_full_subscription
                                   , max(sum_in_rubles_full_subscription - wapi_transactions_in_rubles) AS subscription_sum
                                   , max(wapi_discount_for_partners)                                    AS wapi_discount_for_partners
                                   , max(is_subscription_currency_the_same_as_billing_affilate)         AS is_subscription_currency_the_same_as_billing_affilate
                              FROM billing_affiliate_deduplicated
                              WHERE (has_partner_paid = 1
                                      OR (has_partner_paid = 0 AND subscription_owner = real_money_account_id))
                              GROUP BY 1, 2, 3, 4, 5
                              ),

--этот подзапрос нужен в том случае, если пользователь оплачивает подписку переводом и хорошими бонусами (около 4 оплат)
     balance_spending_standart AS (
                              SELECT subscription_owner                                                 AS account_id
                                   , occured_date
                                   , subscription_id
                                   , subscription_update_id
                                   , action
                                   , max(partner_discount)                                              AS partner_discount
                                   , max(good_balance_spent)                                            AS good_balance_spent
                                   , max(wapi_transactions_in_rubles)                                   AS wapi_transactions_in_rubles
                                   , max(sum_in_rubles_full_subscription)                               AS sum_in_rubles_full_subscription
                                   , max(sum_in_rubles_full_subscription - wapi_transactions_in_rubles) AS subscription_sum
                                   , max(wapi_discount_for_partners)                                    AS wapi_discount_for_partners
                                   , max(is_subscription_currency_the_same_as_billing_affilate)         AS is_subscription_currency_the_same_as_billing_affilate
                              FROM billing_affiliate_deduplicated
                              WHERE has_partner_paid = 2
                              GROUP BY 1, 2, 3, 4, 5
                              ),

     all_balance_spending AS (
                              SELECT *
                              FROM balance_spending_partner_and_client
                              UNION ALL
                              SELECT *
                              FROM balance_spending_standart
                              ),
     good_balance_spent_aggregated AS (
                              SELECT account_id                 -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
                                   , occured_date               -- Дата изменения
                                   , subscription_id            -- ID подписки
                                   , action                     -- Какое изменение оплачено?
                                   , subscription_update_id     -- ID изменения, соответствует guid из subscriptionUpdates
                                   , max(partner_discount)                                       AS partner_discount                    -- Скидка партнера
                                   , sum(subscription_sum)                                       AS subscription_sum_only               -- Сумма оплаты подписки
                                   , sum(wapi_transactions_in_rubles)                            AS wapi_transactions_in_rubles         -- Сумма пополнения баланса WABA в рублях
                                   , sum(sum_in_rubles_full_subscription)                        AS sum_in_rubles_full_subscription     -- Сумма оплаты подписки в рублях вместе с балансом WABA
                                   , sum(good_balance_spent)                                     AS good_balance_spent                  -- Сумма потраченных хороших бонусов
                                   , max(wapi_discount_for_partners)                             AS wapi_discount_for_partners          -- Комиссия партнера за пополнение баланса WABA
                                   , max(is_subscription_currency_the_same_as_billing_affilate)  AS is_subscription_currency_the_same_as_billing_affilate   -- Валюта подписки такая же, как в  billingAffiliate?
                              FROM all_balance_spending
                              GROUP BY 1, 2, 3, 4, 5
                              ),
     good_balance_aggregated AS (
                              SELECT *
                                   , (1 - coalesce(partner_discount, 0)) * subscription_sum_only AS subscription_sum
                              FROM good_balance_spent_aggregated
                              )
SELECT *    -- Таблица платежей с суммой реально потраченных денег
FROM good_balance_aggregated