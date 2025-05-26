WITH registration_sources AS (
                             SELECT *
                             FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_attribution_data`
                             WHERE account_type_current = 'standart'
                             ),
     onboarding_users AS (
                         SELECT DISTINCT account_id
                         FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts_events__role_choose`
                         WHERE crm != 'noCrm'
                         ),
     first_channels AS (
                       SELECT *
                       FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_first_channel_date_and_transport`
                       ),
     first_integration AS (
                          SELECT *
                          FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_first_integration_date_and_type`
                          ),
     first_subscription AS (
                           SELECT *
                           FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_first_subscription_date_and_type`
                           ),
     first_message AS (
                      SELECT *
                      FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_minIncomingMessage`
                      ),
     channels_before_first_subscription AS (
                                           SELECT *
                                           FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_channels_added_before_first_subscription`
                                           ),
     revenue AS (
                SELECT account_id
                     , sum(sum_in_rubles_spent_on_subscription) AS revenue
                FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_and_wapi_sessions_real_money`
                GROUP BY 1
                ),
     profile_info AS (
                     SELECT *
                     FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
                     ),
     channels_by_account_id_n_days_after_registration AS (
                                                         SELECT *
                                                         FROM `dwh-wazzup`.`dbt_nbespalov`.`int_channels_by_account_id_n_days_after_registration`
                                                         ),
     userflow_data_up_to_14_05 AS (
                                  SELECT *
                                  FROM dwh-wazzup.views.userflow_up_to_05_14
                                  ),
     all_user_data AS (
                      SELECT registration_sources.*
                           , first_integration.created_date                                             AS integration_created_date -- дата создания первой интеграции
                           , first_integration.integration_type         -- тип первой интеграции
                           , first_integration.integration_type_valid   -- тип первой валидной интеграции
                           , first_integration.integration_type_valid_created_date  -- дата создания первой валидной интеграции
                           , first_channels.created_date                                                AS channel_created_date -- дата создания канала. Считаем канал созданным только в том случае, если он перешел в temporary=False (например, отсканировали qr код в случае whatsapp)
                           , first_channels.transport                   -- транспорт первого канала
                           , first_message.min_message_date             -- дата отправки первого сообщения через crm
                           , first_subscription.start_date                                              AS min_subscription_date    -- дата первой подписки
                           , first_subscription.subscription_type       -- тип первой подписки
                           , first_subscription.period                  -- длительность первой подписки
                           , first_subscription.tariff                  -- тариф первой подписки
                           , first_subscription.quantity                -- число каналов в первой подписке
                           , channels_before_first_subscription.transport_added    -- транспорты, добавленные до покупки первой подписки
                           , profile_info.russian_country_name          -- Название страны на русском языке
                           , profile_info.currency                                                      AS account_currency -- Валюта
                           , profile_info.region_international          -- Регион
                           , profile_info.account_language              -- Язык аккаунта
                           , revenue.revenue                            -- выручка, принеснная этим аккаунтом, за все время
                           , profile_info.account_currency_by_country   -- Валюта по стране
                           , company_role                               -- Роль в компании, выбранная по опросу во время онбординга
                           , crm_from_survey                            -- CRM, выбранная по опросу во время онбординга
                           , channels_by_account_id_n_days_after_registration.*EXCEPT(account_id)
                           , CASE WHEN onboarding_users.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS passed_onboarding
                      FROM registration_sources
                          LEFT JOIN first_channels
                                  ON registration_sources.account_id = first_channels.account_id
                          LEFT JOIN first_integration
                                  ON registration_sources.account_id = first_integration.account_id
                          LEFT JOIN first_subscription
                                  ON registration_sources.account_id = first_subscription.account_id
                          LEFT JOIN first_message
                                  ON registration_sources.account_id = first_message.account_id
                          LEFT JOIN channels_before_first_subscription
                                  ON registration_sources.account_id = channels_before_first_subscription.account_id
                          LEFT JOIN revenue
                                  ON registration_sources.account_id = revenue.account_id
                          LEFT JOIN profile_info
                                  ON registration_sources.account_id = profile_info.account_id
                          LEFT JOIN channels_by_account_id_n_days_after_registration
                                  ON channels_by_account_id_n_days_after_registration.account_id
                                  = registration_sources.account_id
                          LEFT JOIN userflow_data_up_to_14_05
                                  ON userflow_data_up_to_14_05.account_id
                                  = cast(registration_sources.account_id AS string)
                          LEFT JOIN onboarding_users ON registration_sources.account_id = onboarding_users.account_id
                      ) -- Основные этапы воронки онбординга пользователя: от регистрации до оплаты
SELECT *
FROM all_user_data
WHERE NOT exists (
                 SELECT account_id
                 FROM profile_info
                 WHERE is_employee
                   AND profile_info.account_id = all_user_data.account_id
                 )