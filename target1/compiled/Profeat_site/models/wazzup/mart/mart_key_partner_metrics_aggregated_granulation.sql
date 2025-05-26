WITH metrics_info AS (
    SELECT 
        account_id                 AS partner_id,
        'register'                 AS event,
        currency,
        CAST(NULL AS string)       AS segments_aggregated,
        MIN(partner_register_date) AS date,
        SUM(CAST(NULL AS float64)) AS sum,
        NULL                       AS sum_waba,
        SUM(CAST(NULL AS float64)) AS original_sum,
        SUM(CAST(NULL AS float64)) AS waba_sum_in_rubles,
        SUM(CAST(NULL AS float64)) AS waba_original_sum
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
    GROUP BY 1,2,3,4

UNION ALL 

    SELECT 
        rww.account_id                  AS partner_id,
        'payment'                       AS event,
        api.currency                    AS currency,
        CAST(null AS string)            AS segments_aggregated,
        MIN(paid_date)                  AS date,
        SUM(sum_in_rubles)              AS sum,
        NULL                            AS sum_waba,
        SUM(CAST(NULL AS float64))      AS original_sum,
        SUM(CAST(NULL AS float64))      AS waba_sum_in_rubles,
        SUM(CAST(NULL AS float64))      AS waba_original_sum
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_with_waba` rww
    JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` api ON api.account_id = rww.account_id
    WHERE segment_type in ('of-partner','tech-partner')
    GROUP BY 1,2,3,4


UNION ALL 

    SELECT 
        maa.partner_id,
        'partner_50'          AS event,
        api.currency,
        CAST(NULL AS string)  AS segments_aggregated,
        maa.occured_date      AS date,
        NULL                  AS sum,
        NULL                  AS sum_waba,
        NULL                  AS original_sum,
        CAST(NULL AS float64) AS waba_sum_in_rubles,
        CAST(NULL AS float64) AS waba_original_sum
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_fifty_discount_by_month_and_account` maa
    JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` api ON api.account_id = maa.partner_id

UNION ALL

    SELECT 
        CASE WHEN segment_type = 'tech-partner' THEN mart_revenue_by_segments.account_id
        WHEN partner_type = 'tech-partner' THEN mart_revenue_by_segments.partner_id END     AS partner_id,
        'tech_partner_payment'                                                              AS event,
        int_accounts_profile_info.currency,
        CAST(NULL AS string)                                                                AS segments_aggregated,
        MIN(paid_date)                                                                      AS date, 
        SUM(CAST(NULL AS float64))                                                          AS sum,
        NULL                                                                                AS sum_waba,
        SUM(CAST(NULL AS float64))                                                          AS original_sum,
        SUM(CAST(NULL AS float64))                                                          AS waba_sum_in_rubles,
        SUM(CAST(NULL AS float64))                                                          AS waba_original_sum
    FROM `dwh-wazzup`.`dbt_nbespalov`.`mart_revenue_by_segments` mart_revenue_by_segments
    JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` int_accounts_profile_info ON int_accounts_profile_info.account_id = CASE WHEN segment_type = 'tech-partner' THEN mart_revenue_by_segments.account_id
        WHEN mart_revenue_by_segments.partner_type = 'tech-partner' THEN mart_revenue_by_segments.partner_id END
    WHERE segments_aggregated = 'tech-partner' 
    GROUP BY 1,2,3,4

UNION ALL 

    SELECT 
        partner_id,
        'discount'              AS event,
        currency,
        CAST(NULL AS string)    AS segments_aggregated,
        paid_date               AS date,
        discount_sum_in_rubles  AS sum,
        NULL                    AS sum_waba,
        discount_sum_original   AS original_sum,
        CAST(NULL AS float64)   AS waba_sum_in_rubles,
        CAST(NULL AS float64)   AS waba_original_sum
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_discounts_by_day_and_account`

UNION ALL 

    SELECT 
        account_id                              AS partner_id,
        'reward'                                AS event,
        currency,
        CAST(NULL AS string)                    AS segments_aggregated,
        occured_date_sub                        AS date,
        sum_in_rubles                           AS sum, 
        sum_in_rubles_waba                      AS sum_waba,
        (original_sum + original_sum_waba)      AS original_sum,
        CAST(NULL AS float64)                   AS waba_sum_in_rubles,
        CAST(NULL AS float64)                   AS waba_original_sum
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_reward_by_rewardtype_by_date`

UNION ALL

    SELECT 
        (CASE WHEN account_type in ('partner','tech-partner', 'tech-partner-postpay') THEN account_id 
              WHEN partner_type IN ('tech-partner', 'tech-partner-postpay') THEN partner_id END) AS partner_id,
        'revenue'                                                                                AS event,
        currency,
        (CASE WHEN segment_type ='of-partner' THEN 'partner' 
            WHEN segment_type IN ('tech-partner', 'tech-partner-client') THEN 'tech-partner' 
            WHEN segment_type = 'tech-partner-postpay' THEN 'tech-partner-postpay' END) AS segments_aggregated,
        paid_date                                                                                AS date,
        SUM(sum_in_rubles)                                                                       AS sum,
        NULL                                                                                     AS sum_waba,
        SUM(original_sum)                                                                        AS original_sum,
        SUM(waba_sum_in_rubles)                                                                  AS waba_sum_in_rubles,
        SUM(waba_original_sum)                                                                   AS waba_original_sum
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_with_waba`
    WHERE segment_type IN ('of-partner','tech-partner', 'tech-partner-postpay') OR partner_type IN ('tech-partner', 'tech-partner-postpay')
    GROUP BY 1,2,3,4,5
),

top_100 AS (
    SELECT 
      account_id,
      current_quarter
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_top_100_ru_of_partners`
) 
    -- Агрегированные метрики партнерки
SELECT 
    metrics_info.partner_id, -- Аккаунт партнера, для которого собираются метрики
    metrics_info.event,             -- reward - реферальные выплаты партнеру за подписки, payment - первое пополнение ЛК партнером (когда он был в статусе партнера), revenue - выручка при пополнении ЛК, discount - скидка при оплате подписки, register - дата регистрации партнера, partner_50 - первая оплата подписки при скидке 50%, tech_partner_payment - первая оплата подписки тех. партнером либо его дочкой
    metrics_info.currency,          -- валюта для оплат (revenue, discount)
    metrics_info.date,              -- дата
    metrics_info.sum,               -- сумма для reward
    metrics_info.sum_waba,          -- сумма для waba reward
    metrics_info.original_sum,      -- сумма в валюте для revenue и discount
    metrics_info.waba_sum_in_rubles,-- сумма пополнения баланса вабы в рублях
    metrics_info.waba_original_sum, -- сумма пополнения баланса вабы в валюте
    profile_info.region_type,       -- регион (СНГ, НЕ-СНГ, Неизвестно)
    profile_info.country,           -- Страна
    COALESCE(segments_aggregated, profile_info.type) AS  account_type,  -- тип аккаунта партнера (оф. партнер, тех. партнер)
    profile_info.currency AS partner_currency,                          -- валюта ЛК партнера   
    (CASE WHEN top_100.account_id IS NOT NULL THEN True ELSE False END) AS is_top_100   -- есть ли этот партнер в топ-100
FROM metrics_info
LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info ON metrics_info.partner_id=profile_info.account_id 
LEFT JOIN  top_100 ON top_100.account_id=metrics_info.partner_id AND DATE_TRUNC(metrics_info.date,quarter) = top_100.current_quarter
WHERE is_employee IS FALSE