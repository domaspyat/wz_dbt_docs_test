WITH onboarding_users AS ( /*
 Когда новый пользователь попадает впервые на регистрацию, после того как он зарегался и попал уже во внутрь прилы появляется окно онбординга. 
 В нём 2 листа, в первом нужно выбрать роль (директор, продажник, роп и т.д) 
 на втором какую срм используют (важно: там несколько вариантов и мы не считаем тех, кто нажал что нет никакой срм)*/
                         SELECT account_id
                         FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts_events__role_choose`
                         WHERE crm != 'noCrm'
                         ),
     profile_info AS ( --данные по аккаунтам
                         SELECT account_id
                              , utm_source
                              , utm_medium
                              , register_date
                              , registration_source
                              , registration_source_agg
                              , account_language
                              , currency
                              , region
                              , russian_country_name
                              , region_type
                         FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
                         ),
     revenue AS ( --вся выручка с пользователей с учетом вабы
                         SELECT account_id
                              , sum(sum_in_rubles + waba_sum_in_rubles) AS sum_overall  -- Общая сумма (подписки+ваба баланс)
                         FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_with_waba`
                         GROUP BY account_Id
                         ),
     google_ads AS ( --расходы с google ads
                         SELECT _DATA_DATE                               AS DATE
                              , sum(metrics_clicks)                      AS clicks
                              , sum(metrics_cost_micros * RUR) / 1000000 AS cost
                         FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_google_ads` ads
                        JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates
                         ON ads._DATA_DATE=exchange_rates._ibk AND exchange_rates.currency = 'USD'
                         GROUP BY 1
                         ),
     all_users_regs_onboarding_and_money AS (
                         SELECT DISTINCT utm_source                             -- Извлечение UTM source из URL, по которому зарегистрировался клиент
                                       , utm_medium                             -- Извлечение UTM medium из URL, по которому зарегистрировался клиент
                                       , register_date    AS date               -- Рассматриваемая дата
                                       , registration_source                    -- Источник регистрации
                                       , registration_source_agg                -- Источник регистрации после группировки
                                       , account_language                       -- Язык аккаунта
                                       , currency                               -- Валюта
                                       , russian_country_name                   -- Название страны на русском языке
                                       , region                                 -- Регион
                                       , region_type                            -- Тип региона
                                       , all_u.account_id AS regs_acc           -- Количество регистраций
                                       , onb.account_id   AS passed_onboarding  -- Количество прошедших онбординг
                                       , sum_overall      AS sum_overall        -- Общая сумма (подписки+ваба баланс)
                         FROM profile_info all_u
                             LEFT JOIN onboarding_users onb ON all_u.account_id = onb.account_id
                             LEFT JOIN revenue ON all_u.account_id = revenue.account_id
                         ),
     marketing_data_pre AS ( --данные из яндекс метрики
                         SELECT CASE WHEN traffic_source = 'Search engine traffic' AND traffic_source_detailed = 'Google'             THEN 'google'
                                     WHEN traffic_source = 'Search engine traffic' AND traffic_source_detailed = 'Yandex'             THEN 'yandex'
                                     ELSE utm_source END                                                          AS utm_source
                              , CASE WHEN traffic_source = 'Search engine traffic' THEN 'cpc'
                                     ELSE utm_medium END                                                          AS utm_medium
                              , cast(md.date AS date)                                                             AS date
                              , sum(visits)                                                                       AS visits -- Количество посещений из яндекс метрики
                              , sum(users)                                                                        AS users  -- Количество юзеров из яндекс метрики
                         FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_yandex_metrika_data` md
                         GROUP BY 1, 2, 3
                         ),
     marketing_data AS ( --данные из яндекс метрики + расходы яндекс директа и гугл эдс
                         SELECT md.*
                              , sum(coalesce(yd.cost, 0) + coalesce(ga.cost, 0)) AS cost    -- Стоимость яндекс директа + гугл эдс
                         FROM marketing_data_pre md
                             LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_yandex_direct` yd
                                                     ON md.date = yd.date AND md.utm_medium = 'cpc' AND utm_source = 'yandex'
                             LEFT JOIN google_ads ga ON md.date = ga.date AND md.utm_medium = 'cpc' AND utm_source = 'google'
                         GROUP BY 1, 2, 3, 4, 5
                         )
SELECT *    -- Отчет для маркетинга
     , CASE WHEN utm_medium = 'cpc'     THEN 'Платный трафик'
            WHEN utm_medium = 'organic' THEN 'Поисковый трафик'
            ELSE utm_medium END AS traffic_high_level --верхнеуровнево разбиваем по типу траффика, логика от коллег из маркетинга
     , CASE WHEN utm_medium = 'cpc' AND utm_source = 'yandex'     THEN 'Яндекс.Директ'
            WHEN utm_medium = 'cpc' AND utm_source = 'google'     THEN 'Google Ads'
            WHEN utm_medium = 'cpc' AND utm_source = 'tg'         THEN 'TG Ads'
            WHEN utm_medium = 'cpc'                               THEN utm_source
            WHEN utm_medium = 'organic' AND utm_source = 'direct' THEN 'Яндекс'
            WHEN utm_medium = 'organic' AND utm_source = 'google' THEN 'Google'
            ELSE utm_source END AS traffic_low_level --более детальная разбивка по типу траффика, логика от коллег из маркетинга
FROM marketing_data
    LEFT JOIN all_users_regs_onboarding_and_money USING (utm_source, utm_medium, date)