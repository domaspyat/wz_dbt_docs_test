WITH first_payment_date_and_sum_in_rubles_partner AS (
  SELECT * 
  FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_first_payment_date_and_sum_in_rubles_by_segment_and_partner`
  WHERE segment_type IN ('of-partner','tech-partner')
),

-- first_payment_date_and_sum_in_rubles_child AS (
--   SELECT * 
--   FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_first_payment_date_and_sum_in_rubles_by_segment_and_partner`
--   WHERE segment_type IN ('of-partner-client','tech-partner-client')
-- ),

first_payment_date_and_sum_in_rubles_child_and_partner AS (
  SELECT * 
  FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_first_payment_date_and_sum_in_rubles_by_segment_and_partner`
  WHERE segment_type IN ('of-partner-client','tech-partner-client')
),

last_payment_info AS (
    SELECT *     
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_who_paid_for_client_subscription_with_sum`
),

first_subscription_date AS (
    SELECT * FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_first_subscription_date_and_type`
), --дата первой подписки и тип первой подписки,

subscription_paid_by_partner AS (
    SELECT * FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_subscription_paid_for_client`
),

partner_lk_payments AS (
    SELECT * 
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_first_payment_billing`
),

partner_data AS (
  SELECT 
    account_id,
    register_date,
    country,
    region_type,
    partner_register_date,
    currency,
    type,
    email,
    russian_country_name,
    phone,
    demo_account,
    name
  FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
    WHERE type IN ('partner','tech-partner')
),

partners_and_children_current_and_all_time AS (
  SELECT partner_id, 
    account_id AS child_id,  
    min(start_date) AS created_date, 
    max(end_date) AS max_child_date  
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated`
  WHERE partner_id is not null
  GROUP BY 1,2
),

paid_subscriptions AS (
   SELECT 
      bp.account_id, 
      max(case when state='active' then True else False end) AS has_active_paid_subscription,
      max(cast(paid_at AS date)) AS max_subscription_paid_date
   FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` bp
   WHERE paid_at is not null
   GROUP BY bp.account_id
),

active_integrations AS (
   SELECT 
        account_id, 
        active_integration_name,
        domain 
   FROM `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_active_groupped_by_accounts_type`
),

partner_last_reward_month AS (
  SELECT
    DATE_TRUNC(MAX(occured_at), month) AS last_reward_month,
    account_id, 
FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate`
WHERE object IN ('reward', 'rewardWaba', 'noReward')
GROUP BY 2
),

reward_sum_last_month AS (
  SELECT
    ba.account_id,
    SUM(COALESCE(sum * cor_rate, sum)) AS reward_sum_last_month
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN partner_last_reward_month plrm ON ba.account_id = plrm.account_id AND DATE_TRUNC(ba.occured_at, month) = plrm.last_reward_month
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates` er ON er.data = ba.occured_date AND er.currency = ba.currency AND nominal = 'RUR'
  WHERE object IN ('reward', 'rewardWaba', 'noReward')
  GROUP BY 1
),

partner_reward_all_time_and_last_month AS (
  SELECT
    rslm.*,
    SUM(COALESCE(sum * cor_rate, sum)) AS reward_all_time
  FROM reward_sum_last_month rslm
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba ON rslm.account_id = ba.account_id
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates` er ON er.data = CURRENT_DATE AND er.currency = ba.currency AND nominal = 'RUR'
  WHERE object IN ('reward', 'rewardWaba')
  GROUP BY 1, 2
),

partner_info_aggregated AS (
  SELECT 
    partner_data.account_id                                                     AS partner_id,                                      -- аккаунт партнера
    partner_data.country                                                        AS country,                                         -- страна
    partner_data.region_type                                                    AS region_type,                                     -- Регион. Определяется по стране (СНГ, Международные, Неизвестно)
    partner_data.register_date                                                  AS register_date,                                   -- Дата регистрации аккаунта
    partner_data.partner_register_date                                          AS partner_register_date,                           -- Дата получения партнерки
    partner_data.currency                                                       AS currency,                                        -- Валюта
    partner_data.type                                                           AS type,                                            -- Тип: тех. партнер или оф. партнер
    partner_data.email                                                          AS email,                                           -- почта партнера
    partner_data.name                                                           AS name,                                            -- имя партнера
    partner_data.russian_country_name                                           AS russian_country_name,                            -- страна (русское название)
    partner_data.phone                                                          AS phone,                                           -- телефон
    partners_and_children_current_and_all_time.child_Id                         AS child_Id,                                        -- аккаунт дочки
    partners_and_children_current_and_all_time.created_date                     AS child_created_date,                              -- дата прикрепления дочки к этому партнеру
    partners_and_children_current_and_all_time.max_child_date                   AS max_child_date,                                  -- дата открепления дочки от партнера. в случае, если дочка еще прикреплена к партнеру, будет сегодняшняя дата
    paid_subscriptions.has_active_paid_subscription                             AS has_active_paid_subscription,                    -- есть активная опаченная подписка
    paid_subscriptions.max_subscription_paid_date                               AS max_subscription_paid_date,                      -- последний раз, когда была оплачена подписка дочки (партнером или самой дочкой)
    active_integrations.active_integration_name                                 AS integration_type,                                -- тип интеграции
  --  first_payment_date_and_sum_in_rubles_child.first_payment_date               AS first_revenue_date_child,                        -- первая оплата от аккаунта дочки
  --  first_payment_date_and_sum_in_rubles_child.sum_in_rubles                    AS sum_in_rubles_child,                             -- выручка от дочки
    first_payment_date_and_sum_in_rubles_partner.first_payment_date             AS first_revenue_date_partner,                      -- первая оплата партнера
    first_payment_date_and_sum_in_rubles_partner.sum_in_rubles                  AS sum_in_rubles_partner,                           -- выручка от партнера
    partner_lk_payments.first_billing_date_partner                              AS first_billing_date_partner,                      -- дата первого пополнения ЛК
    partner_lk_payments.billing_sum                                             AS billing_sum,                                     -- сумма, на которую партнер пополнил ЛК за все время
    first_subscription_date.start_date                                          AS first_subscription_date_child,                   -- дата повления первой оплаченной подписки у дочки
    first_payment_date_and_sum_in_rubles_child_and_partner.first_payment_date   AS first_payment_date_child_at_partner,             -- первая дата оплаты
    first_payment_date_and_sum_in_rubles_child_and_partner.sum_in_rubles        AS sum_in_rubles_child_at_partner,                  -- выручка в рублях от дочки в тот период, когда она была прикреплена к партнеру
    subscription_paid_by_partner.balance_spent_by_partner                       AS balance_spent_by_partner,                        -- сколько было списано с баланса партнера на дочку
    subscription_paid_by_partner.good_balance_spent_by_partner                  AS good_balance_spent_by_partner,                   -- сколько было списано хорошего баланса партнера на дочку
    subscription_paid_by_partner.good_balance_spent_by_partner_on_subscription  AS good_balance_spent_by_partner_on_subscription,   -- сколько было списано хорошего баланса партнера на подписку дочки
    subscription_paid_by_partner.good_balance_spent_by_partner_on_waba_balance  AS good_balance_spent_by_partner_on_waba_balance,   -- сколько было списано хорошего баланса партнера на ваба баланс дочки
    children_info.account_segment_type                                          AS account_segment_type,                            -- сегмент для дочки (платит сама или через партнера). определяется по последней оплате
    children_info.email                                                         AS child_email,                                     -- почта дочки
    COALESCE(affiliates_info.name,children_info.name)                           AS child_name,                                      -- имя дочки
    affiliates_info.partner_id                                                  AS current_child_partner,                           -- текущий партнер дочки
    active_integrations.domain                                                  AS domain,                                          -- домен интеграции дочки
    children_info.register_date                                                 AS child_register_date,                             -- дата регистрации дочки
    last_sum_in_month_with_active_subs                                          AS last_sum_in_month_with_active_subs,              -- сумма подписок за последний месяц активности дочки
    lpi.who_paid                                                                AS who_paid,                                        -- кто платил последний раз за дочку
    reward_sum_last_month                                                       AS reward_sum_last_month,                           -- реферальное вознаграждение партнера за последний календарный месяц в рублях
    reward_all_time                                                             AS reward_all_time,                                 -- реферальное вознаграждение партнера за всё время в рублях (берется курс на сегодня)
    subscription_sum                                                            AS subscription_sum_client,                         -- сумма, которую дочка потратила на подписки сама в рублях
    waba_sum_without_bonus                                                      AS waba_sum_without_bonus_client,                    -- сумма, которую дочка потратила на баланс вабы сама в рублях
    bad_balance_spent_on_subscription                                           AS bad_balance_spent_on_subscription,               -- сумма плохого баланса, который дочка потратила на свои подписки
    bad_balance_spent_on_waba_balance                                           AS bad_balance_spent_on_waba_balance                -- сумма плохого баланса, который дочка потратила на свой ваба баланс
  FROM partner_data
  LEFT JOIN partners_and_children_current_and_all_time ON partner_data.account_Id=partners_and_children_current_and_all_time.partner_Id AND partners_and_children_current_and_all_time.child_Id!=partner_data.demo_account
  LEFT JOIN paid_subscriptions ON partners_and_children_current_and_all_time.child_Id=paid_subscriptions.account_Id
  LEFT JOIN active_integrations ON active_integrations.account_id=partners_and_children_current_and_all_time.child_Id
  LEFT JOIN partner_lk_payments ON partner_data.account_id=partner_lk_payments.account_id
 -- LEFT JOIN first_payment_date_and_sum_in_rubles_child ON partners_and_children_current_and_all_time.child_id = first_payment_date_and_sum_in_rubles_child.account_Id
  LEFT JOIN first_payment_date_and_sum_in_rubles_partner ON partner_data.account_id=first_payment_date_and_sum_in_rubles_partner.account_id        
  LEFT JOIN first_subscription_date ON partners_and_children_current_and_all_time.child_id=first_subscription_date.account_id        
  LEFT JOIN first_payment_date_and_sum_in_rubles_child_and_partner ON first_payment_date_and_sum_in_rubles_child_and_partner.account_id=partners_and_children_current_and_all_time.child_id AND    first_payment_date_and_sum_in_rubles_child_and_partner.partner_id=partner_data.account_id
  LEFT JOIN subscription_paid_by_partner ON subscription_paid_by_partner.partner_id=partner_data.account_id AND subscription_paid_by_partner.client_id=partners_and_children_current_and_all_time.child_id        
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` children_info ON children_info.account_id=partners_and_children_current_and_all_time.child_id    
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates` affiliates_info ON affiliates_info.child_id=partners_and_children_current_and_all_time.child_id
  LEFT JOIN last_payment_info lpi ON lpi.account_id = partners_and_children_current_and_all_time.child_id
  LEFT JOIN partner_reward_all_time_and_last_month ON partner_reward_all_time_and_last_month.account_id = partners_and_children_current_and_all_time.partner_Id
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_clients_without_bad_balance` ON int_payments_revenue_clients_without_bad_balance.account_id = partners_and_children_current_and_all_time.child_id AND int_payments_revenue_clients_without_bad_balance.partner_id = partners_and_children_current_and_all_time.partner_id
)
    -- Данные по партнерам и их дочкам: страна, дата регистрации партнера, дата прикрепления и открепления дочки, оплаты и выручка
SELECT * 
FROM partner_info_aggregated