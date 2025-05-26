WITH onboarding_users AS (
                         SELECT account_id
                         FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts_events__role_choose`
                         WHERE crm != 'noCrm'
                         ),
     profile_info AS (
                         SELECT account_id
                              , utm_source
                              , utm_campaign
                              , utm_content
                              , register_date
                         FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
                         ),
     revenue AS (
                         SELECT account_id
                              , sum(sum_in_rubles + waba_sum_in_rubles) AS sum_overall
                         FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_with_waba`
                         GROUP BY account_Id
                         ),
     all_users_regs_onboarding_and_money AS (
                         SELECT utm_campaign                                            -- Извлечение UTM campaign из URL, по которому зарегистрировался клиент
                              , register_date                    AS date                -- Рассматриваемая дата
                              , count(DISTINCT all_u.account_id) AS regs_count          -- Количество регистраций
                              , count(DISTINCT onb.account_id)   AS passed_onboarding   -- Количество прошедших онбординг
                              , sum(sum_overall)                 AS sum_overall         -- Общая сумма покупок клиента
                         FROM profile_info all_u
                             LEFT JOIN onboarding_users onb ON all_u.account_id = onb.account_id
                             LEFT JOIN revenue ON all_u.account_id = revenue.account_id
                         GROUP BY utm_campaign, register_date
                         ),
     direct_data AS (
                         SELECT   date                          -- Рассматриваемая дата
                                , utm_campaign                  -- Извлечение UTM campaign из URL, по которому зарегистрировался клиент
                                , campaign_name                 -- Название рекламы
                                , sum(impressions) impressions  -- Показы рекламы
                                , sum(clicks) AS clicks         -- Клики на рекламу
                                , sum(COST) COST                -- Стоимость
                         FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_yandex_direct`
                         GROUP BY 1, 2, 3
                         )
SELECT *    -- Информация по рекламе в яндекс директ
FROM direct_data
    LEFT JOIN all_users_regs_onboarding_and_money USING (date, utm_campaign)
    --