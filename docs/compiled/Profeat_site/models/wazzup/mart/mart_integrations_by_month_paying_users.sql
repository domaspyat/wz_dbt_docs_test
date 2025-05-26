with integration_subscription_info as (
    select * from 
    `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_with_type_by_account_id_only_paid_and_promised_payment`
    where rn=1
    ),

integration_by_month_deduplicated_with_integration_type as ( select *,
 (case 
    when integration_type_with_api is null then 'no_integration'
    when integration_type_with_api='Гетлу' then 'Getcourse'
    when integration_type_with_api='getloo' then 'Getcourse'
    when integration_type_with_api='gc-messenger' then 'Getcourse'
    when integration_type_with_api='Клиентикс' then 'KLIENTIKS CRM'
    when integration_type_with_api='sbis' then 'СБИС'
    when integration_type_with_api='autodealer' then 'Автодилер'
    when integration_type_with_api='Fitbase' then 'fitbase'
    when integration_type_with_api='moyklass' then 'Moy Klass'
    when integration_type_with_api='getcourseprofi' then 'Getcourse'
    when integration_type_with_api='impulsecrm' then 'Impuls CRM'
    when integration_type_with_api='Альфа CRM' then 'alfacrm'
    when integration_type_with_api='vionvi CRM' then 'vionvi'
    when integration_type_with_api='omnidesk' then 'OmniDesk'
    when integration_type_with_api='stocrm' then 'STOCRM'
    when integration_type_with_api='Omnidesk' then 'OmniDesk'
    when integration_type_with_api='Sport CRM' then 'SportCRM'
    when integration_type_with_api='Клиентская база' then 'clientbase'
    when integration_type_with_api='7384' then 'api'
    when integration_type_with_api='imb-service' then 'RetailCRM'
    when integration_type_with_api='4kzn' then 'api'
    when integration_type_with_api='akfa' then 'api'
    when integration_type_with_api='accelonline' then 'api'
    when integration_type_with_api='appcloud' then 'api'
    when integration_type_with_api='webhook' then 'api'
    when integration_type_with_api='wazzup24' then 'api'
    when integration_type_with_api='beget' then 'api'
    when integration_type_with_api='bronix' then 'api'
    when integration_type_with_api='okk24' then 'api'
    when integration_type_with_api='prime' then 'api'
    when integration_type_with_api='olla' then 'api'
    when integration_type_with_api='synergybot' then 'api'
    when integration_type_with_api='annaver' then 'api'
    when integration_type_with_api='yandexcloud' then 'api'
    else integration_type_with_api
    end) as integration_type_with_api_aggregated        -- тип интеграции с расшифровкой по api - вручную причесанные данные. Ифнормацию о большей части интеграции мы вытаскиваем из доменного имени, а она не всегда несет в себе полезную информацию. В таких случаях мы заменяем на общее название 'api' - то значит, что мы хз, что это за интеграция
    from integration_subscription_info          
),

integrations_with_profile_info as (

select integration_by_month_deduplicated_with_integration_type.*,
last_day(date,month) as last_day_of_month,                  -- Последний день месяца
last_value(integration_type) over (partition by integration_by_month_deduplicated_with_integration_type.account_id, date_trunc(date,month) order by integration_start_date asc
rows between unbounded preceding and unbounded following
) as last_value_integration_type_month,                     -- Последний тип интеграции в месяце
last_value(integration_type_with_api) over (partition by integration_by_month_deduplicated_with_integration_type.account_id, date_trunc(date,month) order by integration_start_date asc
rows between unbounded preceding and unbounded following
) as last_value_integration_type_with_api_month,            -- Последний тип интеграции с расшифровкой api в месяце
last_value(integration_type_with_api_aggregated) over (partition by integration_by_month_deduplicated_with_integration_type.account_id, date_trunc(date,month) order by integration_start_date asc
rows between unbounded preceding and unbounded following
) as last_value_integration_type_with_api_aggregated_month, -- Последний тип интеграции с причесанной расшифровкой api в месяце
profile_info.region_international,              -- Регион
profile_info.russian_country_name,              -- Название страны на русском языке
profile_info.utm_source,                        -- Извлечение UTM source из URL, по которому зарегистрировался клиент
profile_info.utm_campaign,                      -- Извлечение UTM campaign из URL, по которому зарегистрировался клиент
profile_info.utm_medium,                        -- Извлечение UTM medium из URL, по которому зарегистрировался клиент
profile_info.utm_term,                          -- Извлечение UTM term из URL, по которому зарегистрировался клиент
profile_info.utm_content,                       -- Извлечение UTM content из URL, по которому зарегистрировался клиент
profile_info.account_segment_type,              -- сегмент аккаунта
profile_info.currency,                          -- Валюта
profile_info.type,                              -- Тип аккаунта
profile_info.registration_source_current,       -- Детальное указание источника регистрации (для обычных аккаунтов) и текущего типа аккаунта для партнёрских. Приставка current используется так как тут показывается текущий тип аккаунта, а не в момент регистрации
profile_info.registration_source_agg_current    -- Агрегированная версия registration_source_current и account_type_current. Если аккаунт как-то связан с партнёрами, то текущий тип аккаунта, иначе источник регистрации. Приставка current используется так как тут показывается текущий тип аккаунта, а не в момент регистрации

 from integration_by_month_deduplicated_with_integration_type
inner join  `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info 
on profile_info.account_id=integration_by_month_deduplicated_with_integration_type.account_id
where is_employee is distinct from True)
    -- Какая интеграция была у пользователя добавлена в дни активности оплаченной подписки или подписки на обещанном платеже
select * from integrations_with_profile_info