with channels as (
    select account_id,
    case when transport = 'wapi' then 'waba'
         else transport 
    end as transport,
    created_date,
    temporary ,
    guid
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels`
) -- Таблица c каналами по аккаунту через n дней после регистрации
 select profile_info.account_id,    -- ID аккаунта
 
     
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'avito' and temporary=False  then channels.guid end ) avito_7_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'avito'  then channels.guid end ) avito_7_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'avito' and temporary=False  then channels.guid end ) avito_14_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'avito'  then channels.guid end ) avito_14_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'avito' and temporary=False  then channels.guid end ) avito_21_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'avito'  then channels.guid end ) avito_21_channels_all
                     
                 
                ,
                
     
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'waba' and temporary=False  then channels.guid end ) waba_7_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'waba'  then channels.guid end ) waba_7_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'waba' and temporary=False  then channels.guid end ) waba_14_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'waba'  then channels.guid end ) waba_14_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'waba' and temporary=False  then channels.guid end ) waba_21_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'waba'  then channels.guid end ) waba_21_channels_all
                     
                 
                ,
                
     
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'whatsapp' and temporary=False  then channels.guid end ) whatsapp_7_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'whatsapp'  then channels.guid end ) whatsapp_7_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'whatsapp' and temporary=False  then channels.guid end ) whatsapp_14_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'whatsapp'  then channels.guid end ) whatsapp_14_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'whatsapp' and temporary=False  then channels.guid end ) whatsapp_21_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'whatsapp'  then channels.guid end ) whatsapp_21_channels_all
                     
                 
                ,
                
     
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'instagram' and temporary=False  then channels.guid end ) instagram_7_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'instagram'  then channels.guid end ) instagram_7_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'instagram' and temporary=False  then channels.guid end ) instagram_14_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'instagram'  then channels.guid end ) instagram_14_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'instagram' and temporary=False  then channels.guid end ) instagram_21_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'instagram'  then channels.guid end ) instagram_21_channels_all
                     
                 
                ,
                
     
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'tgapi' and temporary=False  then channels.guid end ) tgapi_7_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'tgapi'  then channels.guid end ) tgapi_7_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'tgapi' and temporary=False  then channels.guid end ) tgapi_14_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'tgapi'  then channels.guid end ) tgapi_14_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'tgapi' and temporary=False  then channels.guid end ) tgapi_21_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'tgapi'  then channels.guid end ) tgapi_21_channels_all
                     
                 
                ,
                
     
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'vk' and temporary=False  then channels.guid end ) vk_7_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'vk'  then channels.guid end ) vk_7_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'vk' and temporary=False  then channels.guid end ) vk_14_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'vk'  then channels.guid end ) vk_14_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'vk' and temporary=False  then channels.guid end ) vk_21_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'vk'  then channels.guid end ) vk_21_channels_all
                     
                 
                ,
                
     
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'telegram' and temporary=False  then channels.guid end ) telegram_7_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=7 and transport = 'telegram'  then channels.guid end ) telegram_7_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'telegram' and temporary=False  then channels.guid end ) telegram_14_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=14 and transport = 'telegram'  then channels.guid end ) telegram_14_channels_all
                     ,
                
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'telegram' and temporary=False  then channels.guid end ) telegram_21_channels_not_temporary,
                   count(case when date_diff(channels.created_date, register_date, day) <=21 and transport = 'telegram'  then channels.guid end ) telegram_21_channels_all
                     
                 
                
                  
            from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info 
            left join channels on channels.account_id = profile_info.account_Id  
            group by profile_info.account_id