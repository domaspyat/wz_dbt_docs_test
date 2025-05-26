with accounts as (select 
    * from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
    ),


first_type_partner_and_refparent as (
    select account_id,
    start_date,
     partner_id,
     account_type,
     partner_type,
     refparent_id from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partner_refparent_account_type_partner_type__on_registration_date`
    ),

affiliates as (
    select reflink_code,
    child_id,
    partner_id from `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates`

),

attribution_data as (
    select accounts.account_id, -- ID аккаунта
    accounts.country,           -- Страна
    accounts.utm_source,        -- Извлечение UTM source из URL, по которому зарегистрировался клиент
    accounts.utm_medium,        -- Извлечение UTM medium из URL, по которому зарегистрировался клиент
    accounts.utm_campaign,      -- Извлечение UTM campaign из URL, по которому зарегистрировался клиент
    accounts.utm_term,          -- Извлечение UTM term из URL, по которому зарегистрировался клиент
    accounts.utm_content,       -- Извлечение UTM content из URL, по которому зарегистрировался клиент
    accounts.yandex_id,         -- Метрика Яндекса, заполняемая при регистрации
    accounts.ref,               -- Реферальная ссылка по которой был зарегистрирован аккаунт Может быть в формате ссылки https://wazzup24.com/?utm_p=NqhTp0FR Или только UTM-метка 7i7OIAR2
    accounts.type as account_type_current,          -- Текущий тип аккаунта
    first_type_partner_and_refparent.partner_id,    -- ID аккаунта партнера
    first_type_partner_and_refparent.refparent_id,  -- ID аккаунта реф. партнера
    first_type_partner_and_refparent.account_type,  -- Тип аккаунта
    first_type_partner_and_refparent.partner_type,  -- Тип аккаунта партнера
    start_date as registration_date,                -- Дата регистрации
    (case 
    when account_type='partner' then 'partner'
    when account_type='tech-partner' then 'tech-partner'
    when account_type='child-postpay' then 'child-postpay'
    when account_type='tech-partner-postpay' then 'tech-partner-postpay'
    when affiliates.reflink_code='manual_registration' and first_type_partner_and_refparent.partner_type='partner' then 'manual_registration'
    when affiliates.reflink_code='manual_registration' and first_type_partner_and_refparent.partner_type='standart' then 'manual_registration'
    when partner_type='partner' and  affiliates.reflink_code is distinct from 'manual_registration' then 'partner_code'
    when partner_type='standart' and refparent_id is not null and affiliates.partner_id is not null then 'partner_code'
    when partner_type='tech-partner' then 'tech_partner_code' 
    when (reflink_code is not null) and refparent_id is not null and first_type_partner_and_refparent.partner_type is null then 'referal_code'
    when accounts.utm_source='yandex' then 'yandex_ads'
    when accounts.utm_medium in ('cpc','cpm','supplies') and accounts.utm_source in ('yandex','direct') then 'yandex_ads'
    when accounts.utm_source='in' then 'google_ads'
    when accounts.utm_source='Facebook.ads' then 'facebook_ads'
    when accounts.utm_source='facebook' and accounts.utm_medium LIKE '%India%' then 'facebook_ads' 
    when accounts.utm_medium='cpc' and accounts.utm_source is null then 'ads'
    when accounts.utm_medium in ('cpc','cpm') then  concat(accounts.utm_source, '_ads')
    when (accounts.referrer LIKE '%zoho%') or (accounts.utm_source='zoho') and accounts.referrer not like '%wazzup%' then 'crm_zoho'
    when accounts.utm_source='bitrix'  then 'crm_bitrix'
    when accounts.utm_source='bitrix_int' then 'crm_bitrix'
    when accounts.utm_source='amocrm_ru' then 'crm_amo'
    when accounts.utm_source='amocrm_com' then 'crm_amo'
    when accounts.utm_source='amocrm' then 'crm_amo'
    when accounts.utm_source='kommo' then 'crm_amo'
    when accounts.utm_source like '%kommo%' then 'crm_amo'
    when accounts.utm_source like '%hubspot%' then 'crm_hubspot'
    when accounts.utm_source like '%pipedrive%' then 'crm_pipedrive'
    when accounts.utm_medium LIKE '%youtube%' then 'youtube'
    when accounts.utm_source LIKE '%youtube%' then 'youtube_ads'
    when accounts.utm_medium LIKE '%email' then 'other'
    when ((accounts.referrer LIKE '%amo%') or (accounts.utm_source LIKE '%amo%')) and accounts.referrer not like '%wazzup%'  then 'crm_amo'
    when (accounts.referrer LIKE '%megaplan%') or (accounts.utm_source='megaplan') and accounts.referrer not like '%wazzup%' then 'crm_megaplan'
    when (accounts.referrer LIKE '%hubspot%') or (accounts.utm_source='hubspot') and accounts.referrer not like '%wazzup%' then 'crm_hubspot'
    when accounts.referrer LIKE '%sbis%' then 'crm_sbis'
    when accounts.referrer LIKE '%cleverbox-crm%' then 'crm_cleverbox'
    when accounts.referrer LIKE '%clientbase%' then 'crm_clientbase'
    when accounts.referrer LIKE '%alfacrm%' then 'crm_alfacrm'
    when (accounts.referrer LIKE '%planfix%'  and accounts.referrer not like '%wazzup%') or (accounts.utm_source='planfix') then 'crm_planfix'
    when accounts.referrer LIKE '%stocrm%' then 'crm_stocrm'
    when accounts.referrer LIKE '%omnidesk%' and accounts.referrer not like '%wazzup%' then 'crm_omnidesk'
    when accounts.referrer LIKE '%klientiks%' and accounts.referrer not like '%wazzup%' then 'crm_klientiks'
    when accounts.referrer LIKE '%fitbase%'  and accounts.referrer not like '%wazzup%'then 'crm_fitbase'
    when ((accounts.referrer LIKE '%bitrix%')  or (accounts.utm_source LIKE '%bitrix%') or (accounts.referrer LIKE '%b24%') or (accounts.referrer LIKE '%btx24%')) and ((accounts.referrer not LIKE '%com%') or (accounts.referrer is null) or (accounts.referrer LIKE '%google%')  ) then 'crm_bitrix'
    when ((accounts.referrer LIKE '%bitrix%')  or (accounts.utm_source LIKE '%bitrix%') or (accounts.referrer LIKE '%b24%') or (accounts.referrer LIKE '%btx24%')) and ((accounts.referrer LIKE '%com%' and accounts.referrer not like '%google%') or (accounts.referrer LIKE '%pl%')   ) then 'crm_bitrix'
    when accounts.referrer LIKE '%vk%' then 'other'
    when accounts.referrer LIKE '%android-app%' then 'other'
    when accounts.referrer LIKE '%google%' and accounts.utm_source is null then 'poisk_google'
    when accounts.referrer LIKE '%google%' and accounts.utm_source is null then 'poisk_google'
    when accounts.referrer LIKE '%webpkgcache%' and accounts.utm_source is null then 'poisk_google'
    when accounts.referrer LIKE '%ampproject%' and accounts.utm_source is null then 'poisk_google'
    when accounts.referrer LIKE '%turbopages%' and accounts.utm_source is null then 'poisk_yandex'
    when accounts.referrer LIKE '%yandex%' then 'poisk_yandex'
    when accounts.referrer='null' then 'unknown'
    when REGEXP_CONTAINS(accounts.referrer, r'^https://(wazzup24|wazzup-24).(com|ru|us|in|kz|es)(\/)?$') and accounts.utm_source is null and accounts.utm_medium is null and accounts.utm_campaign is null then 'unknown'
    when accounts.utm_source is not null then 'other'
    when accounts.referrer like '%bing%'   and accounts.ref is null then 'other_poisk'
    when accounts.referrer like '%signup%' and accounts.ref is null  then 'unknown'
    when accounts.referrer like '%yahoo%'   and accounts.ref is null then 'other_poisk'
    when accounts.referrer like '%ysclid%' then 'poisk_yandex'
    when accounts.referrer like '%app.wazzup24.com%'  and accounts.ref is null then 'unknown'
   
    when (accounts.referrer is not null) and accounts.referrer!='' then 'other'
    else 'unknown'
    end
    ) as registration_source,   -- источник регистрации на момент регистрации (может измениться тип аккаунта)

     (case 
    when accounts.type='partner' then 'partner'
    when accounts.type='tech-partner' then 'tech-partner'
    when accounts.type='child-postpay' then 'child-postpay'
    when accounts.type='tech-partner-postpay' then 'tech-partner-postpay'
    when affiliates.reflink_code='manual_registration' and partner_accounts_type.type='partner' then 'manual_registration'
    when affiliates.reflink_code='manual_registration' and partner_accounts_type.type='standart' then 'manual_registration'
    when partner_accounts_type.type='partner' and  affiliates.reflink_code is distinct from 'manual_registration' then 'partner_code'
    when  partner_accounts_type.type='standart' and refparent_id is not null and affiliates.partner_id is not null then 'partner_code'
    when partner_accounts_type.type='tech-partner' then 'tech_partner_code' 
    when (reflink_code is not null) and refparent_id is not null and affiliates.partner_id is null then 'referal_code'
    when accounts.utm_source='yandex' then 'yandex_ads'
    when accounts.utm_medium in ('cpc','cpm','supplies') and accounts.utm_source in ('yandex','direct') then 'yandex_ads'
    when accounts.utm_source='in' then 'google_ads'
    when accounts.utm_source='Facebook.ads' then 'facebook_ads'
    when accounts.utm_source='facebook' and accounts.utm_medium LIKE '%India%' then 'facebook_ads' 
    when accounts.utm_medium='cpc' and accounts.utm_source is null then 'ads'
    when accounts.utm_medium in ('cpc','cpm') then  concat(accounts.utm_source, '_ads')
    when (accounts.referrer LIKE '%zoho%') or (accounts.utm_source='zoho') then 'crm_zoho'
    when accounts.utm_source='bitrix' then 'crm_bitrix'
    when accounts.utm_source='bitrix_int' then 'crm_bitrix'
    when accounts.utm_source='amocrm_ru' then 'crm_amo'
    when accounts.utm_source='amocrm_com' then 'crm_amo'
    when accounts.utm_source='amocrm' then 'crm_amo'
    when accounts.utm_source='kommo' then 'crm_amo'
    when accounts.utm_source like '%kommo%' then 'crm_amo'
    when accounts.utm_source like '%hubspot%' then 'crm_hubspot'
    when accounts.utm_source like '%pipedrive%' then 'crm_pipedrive'
    when accounts.utm_medium LIKE '%youtube%' then 'youtube'
    when accounts.utm_source LIKE '%youtube%' then 'youtube_ads'
    when accounts.utm_medium LIKE '%email' then 'other'
    when ((accounts.referrer LIKE '%amo%') or (accounts.utm_source LIKE '%amo%'))  then 'crm_amo'
    when (accounts.referrer LIKE '%megaplan%') or (accounts.utm_source='megaplan') then 'crm_megaplan'
    when (accounts.referrer LIKE '%hubspot%') or (accounts.utm_source='hubspot') then 'crm_hubspot'
    when accounts.referrer LIKE '%sbis%' then 'crm_sbis'
    when accounts.referrer LIKE '%cleverbox-crm%' then 'crm_cleverbox'
    when accounts.referrer LIKE '%clientbase%' then 'crm_clientbase'
    when accounts.referrer LIKE '%alfacrm%' then 'crm_alfacrm'
    when (accounts.referrer LIKE '%planfix%'  and accounts.referrer not like '%wazzup%') or (accounts.utm_source='planfix') then 'crm_planfix'
    when accounts.referrer LIKE '%stocrm%' then 'crm_stocrm'
    when accounts.referrer LIKE '%omnidesk%' then 'crm_omnidesk'
    when accounts.referrer LIKE '%klientiks%' then 'crm_klientiks'
    when accounts.referrer LIKE '%fitbase%' then 'crm_fitbase'
    when ((accounts.referrer LIKE '%bitrix%')  or (accounts.utm_source LIKE '%bitrix%') or (accounts.referrer LIKE '%b24%') or (accounts.referrer LIKE '%btx24%')) and ((accounts.referrer not LIKE '%com%') or (accounts.referrer is null) or (accounts.referrer LIKE '%google%')  ) then 'crm_bitrix'
    when ((accounts.referrer LIKE '%bitrix%')  or (accounts.utm_source LIKE '%bitrix%') or (accounts.referrer LIKE '%b24%') or (accounts.referrer LIKE '%btx24%')) and ((accounts.referrer LIKE '%com%' and accounts.referrer not like '%google%') or (accounts.referrer LIKE '%pl%')   ) then 'crm_bitrix'
    when accounts.referrer LIKE '%vk%' then 'other'
    when accounts.referrer LIKE '%android-app%' then 'other'
    when accounts.referrer LIKE '%google%' and accounts.utm_source is null then 'poisk_google'
    when accounts.referrer LIKE '%webpkgcache%' and accounts.utm_source is null then 'poisk_google'
    when accounts.referrer LIKE '%ampproject%' and accounts.utm_source is null then 'poisk_google'
    when accounts.referrer LIKE '%yandex%' then 'poisk_yandex'
    when accounts.referrer LIKE '%turbopages%' and accounts.utm_source is null then 'poisk_yandex'
    when accounts.referrer='null' then 'unknown'
    when REGEXP_CONTAINS(accounts.referrer, r'^https://(wazzup24|wazzup-24).(com|ru|us|in|kz|es)(\/)?$') and accounts.utm_source is null and accounts.utm_medium is null and accounts.utm_campaign is null then 'unknown'
    when accounts.utm_source is not null then 'other'
    when accounts.referrer like '%bing%' then 'other_poisk'
    when accounts.referrer like '%ysclid%' then 'poisk_yandex'
    when accounts.referrer like '%signup%' and accounts.ref is null  then 'unknown'
    when accounts.referrer like '%yahoo%' then 'other_poisk'
    when accounts.referrer like '%app.wazzup24.com%'  and accounts.ref is null then 'unknown'
    when (accounts.referrer is not null) and accounts.referrer!='' then 'other'

    
    else 'unknown'
    end
    )  as registration_source_current,  -- агрегированный источник регистрации на текущий момент (может измениться тип аккаунта)
    accounts.referrer                   -- Сторонний сайт, с которого пользователь попал на наш сайт. Может быть null
    from accounts left join first_type_partner_and_refparent on accounts.account_id=first_type_partner_and_refparent.account_id
    left join affiliates on accounts.account_id=affiliates.child_id
    left join accounts partner_accounts_type on affiliates.partner_id=partner_accounts_type.account_id
),

registration_source_aggregated as (
    select *, 
    (case 
    when account_type_current='partner' then 'partner'
    when account_type_current='tech-partner' then 'tech-partner'
    when account_type_current='child-postpay' then 'child-postpay'
    when account_type_current='tech-partner-postpay' then 'tech-partner-postpay'
    when registration_source_current='crm_bitrix' then 'crm_bitrix'
    when registration_source_current='crm_amo' then 'crm_amo'
    when (registration_source_current like '%zoho%') or (registration_source_current like '%planfix%') or (registration_source_current like '%hubspot%') or (registration_source like '%pipedrive%') then 'crm_our'
    when registration_source_current='poisk_google' then 'poisk_google'
    when registration_source_current='poisk_yandex' then 'poisk_yandex'
    when registration_source_current like '%ads%' then 'ads'
    when registration_source_current='referal_code' then 'referal_code'
    when registration_source_current='manual_registration' then 'manual_registration'
    when registration_source_current='partner_code' then 'partner_code'
    when registration_source_current='tech_partner_code' then 'tech_partner_code'
    when registration_source_current='unknown' then 'direct'
    else 'other'
    end)
    as registration_source_agg_current, -- агрегированный источник регистрации на текущий момент (может измениться тип аккаунта)
    
     (case 
    when account_type='partner' then 'partner'
    when account_type='tech-partner' then 'tech-partner'
    when account_type='child-postpay' then 'child-postpay'
    when account_type='tech-partner-postpay' then 'tech-partner-postpay'
    when registration_source='crm_bitrix' then 'crm_bitrix'
    when registration_source='crm_amo' then 'crm_amo'
    when (registration_source like '%zoho%') or (registration_source like '%planfix%') or (registration_source like '%hubspot%') or (registration_source like '%pipedrive%') then 'crm_our'
    when registration_source='poisk_google' then 'poisk_google'
    when registration_source='poisk_yandex' then 'poisk_yandex'
    when registration_source like '%ads%' then 'ads'
    when registration_source='referal_code' then 'referal_code'
    when registration_source='manual_registration' then 'manual_registration'
    when registration_source='partner_code' then 'partner_code'
    when registration_source='tech_partner_code' then 'tech_partner_code'
    when registration_source='unknown' then 'direct'
    else 'other'
    end)
    as registration_source_agg  -- агрегированный источник регистрации на момент регистрации (может измениться тип аккаунта)
    
    
     from  attribution_data),

registration_source_with_account_type as (
    select *, 
    (case when registration_source_agg in ('partner','tech-partner','referal_code','child_postpay','manual_registration','tech_partner_code','tech-partner-postpay','partner_code')
    then registration_source_agg
    else 'standart'
    end
    ) as account_registration_type,         -- Тип аккаунта при регистрации
    (case when registration_source_agg_current in ('partner','tech-partner','referal_code','child_postpay','manual_registration','tech_partner_code','tech-partner-postpay','partner_code')
    then registration_source_agg_current
    else 'standart'
    end
    ) as account_registration_type_current  -- Текущий тип аккаунта при регистрации

     from registration_source_aggregated
)    
    -- Таблица с информацией об источнике регистрации аккаунта
select * from registration_source_with_account_type