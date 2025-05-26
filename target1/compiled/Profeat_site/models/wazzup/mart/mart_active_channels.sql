SELECT date,
  date_trunc(date,week) week,
  date_trunc(date,month) month,
  date_trunc(date,year) year,
  CASE
    WHEN type = 'partner-demo' THEN 'Демо аккаунт'
    WHEN account_type IN ('partner','tech-partner') AND is_free THEN 'В подписке у партнера'
    WHEN type = 'child-postpay' THEN 'Каналы постоплатников'
    WHEN transport = 'whatsapp' AND is_free THEN 'В бесплатной подписке'
    WHEN transport = 'whatsapp' AND subscription_id IS NOT NULL THEN 'В активной подписке'
    WHEN transport = 'whatsapp' AND date <= DATE_ADD(whatsap_trial, INTERVAL 1 DAY) AND subscription_id IS NULL THEN 'В триальной подписке'
    WHEN transport = 'tgapi' AND is_free THEN 'В бесплатной подписке'
    WHEN transport = 'tgapi' AND subscription_id IS NOT NULL THEN 'В активной подписке'
    WHEN transport = 'tgapi' AND date <= DATE_ADD(tgapi_trial, INTERVAL 1 DAY) THEN 'В триальной подписке'
    WHEN transport = 'instagram' AND is_free THEN 'В бесплатной подписке'
    WHEN transport = 'instagram' AND subscription_id IS NOT NULL THEN 'В активной подписке'
    WHEN transport = 'instagram' AND date <= DATE_ADD(instagram_trial, INTERVAL 1 DAY) AND subscription_id IS NULL THEN 'В триальной подписке'
    WHEN transport = 'waba' AND is_free THEN 'В бесплатной подписке'
    WHEN transport = 'waba' AND subscription_id IS NULL AND package_id IS NOT NULL THEN 'В триальной подписке'
    WHEN transport = 'waba' AND subscription_id IS NOT NULL THEN 'В активной подписке'
    WHEN transport = 'waba' AND date <= DATE_ADD(wapi_trial, INTERVAL 1 DAY) AND subscription_id IS NULL THEN 'В триальной подписке'
    WHEN transport = 'avito' AND is_free THEN 'В бесплатной подписке'
    WHEN transport = 'avito' AND subscription_id IS NOT NULL THEN 'В активной подписке'
    WHEN transport = 'avito' AND date <= DATE_ADD(avito_trial, INTERVAL 1 DAY) AND subscription_id IS NULL THEN 'В триальной подписке'
    WHEN transport = 'vk' AND is_free THEN 'В бесплатной подписке'
    WHEN transport = 'vk' AND subscription_id IS NOT NULL THEN 'В активной подписке'
    WHEN transport = 'vk' AND date <= DATE_ADD(vk_trial, INTERVAL 1 DAY) AND subscription_id IS NULL THEN 'В триальной подписке'
    WHEN transport = 'vk' AND package_id IS NULL THEN 'В бесплатной подписке'
    WHEN transport = 'telegram' AND subscription_id IS NOT NULL THEN 'В активной подписке'
    WHEN transport = 'telegram' AND package_id IS NOT NULL THEN 'В бесплатной подписке'
    WHEN transport = 'telegram' AND date <= DATE_ADD(telegram_trial, INTERVAL 1 DAY) AND subscription_id IS NULL THEN 'В триальной подписке'
    ELSE 'В активной подписке'
  END AS channel_status,
  transport,
  COUNT(DISTINCT IF(currency = 'RUR', channel_id, NULL)) AS cnt_rur,
  COUNT(DISTINCT IF(currency = 'EUR', channel_id, NULL)) AS cnt_eur,
  COUNT(DISTINCT IF(currency = 'USD', channel_id, NULL)) AS cnt_usd,
  COUNT(DISTINCT IF(currency = 'KZT', channel_id, NULL)) AS cnt_kzt,
  COUNT(DISTINCT IF(currency IN ('RUR','KZT'), channel_id, NULL)) AS cnt_rurkzt,
  COUNT(DISTINCT IF(currency IN ('USD','EUR'), channel_id, NULL)) AS cnt_usdeur,
  COUNT(DISTINCT channel_id) AS cnt_all

FROM `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_channels`
GROUP BY GROUPING SETS (  
  (channel_status, date, transport),
  (channel_status, week, transport),  
  (channel_status, month, transport),
  (channel_status, year, transport))