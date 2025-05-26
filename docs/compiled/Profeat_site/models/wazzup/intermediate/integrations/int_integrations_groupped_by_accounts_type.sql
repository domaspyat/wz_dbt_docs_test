with accounts_info as (
    Select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`),

specific_integrations as (
   select 
        distinct account_id, 
                 case when integration_type like '%amo%' then 'AMO'
                      when integration_type like  '%bitrix%' then 'Bitrix24'
                      when integration_type like  '%hubspot%' then 'Hubspot'
                      when integration_type like '%zoho%' then 'Zoho'
                      when integration_type like '%pipe%' then 'Pipedrive'  -- тип интеграции еще в разработке вроде
                      when integration_type like '%api%' then 'API'
                  end as integration_type
   from `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_with_type_with_pipedrive`
   where    integration_type like '%amo%'    
             or integration_type like  '%bitrix%' 
             or integration_type like  '%hubspot%'
             or integration_type like '%zoho%'  
             or integration_type like '%pipe%'  
             or integration_type like '%api%'
   )    -- Таблица, которая показывает какой тип интеграции у аккаунта
--account_and_integrations as (
    select profile_info.account_id, -- ID аккаунта
           IFNULL(integration_type,'Не было интеграции') as integration_filter  -- Название интеграции, если она была. Если не было - 'Не было интеграции'
    from accounts_info profile_info
    left join specific_integrations spi on profile_info.account_id = spi.account_id
   -- where profile_info.account_id = 72504447
/*
select account_id
        ,MAX(IF(integration_type = 'AMO', true, false)) as had_amo
        ,MAX(IF(integration_type = 'Bitrix24', true, false)) as had_bitrix
        ,MAX(IF(integration_type = 'Hubspot', true, false)) as had_hubspot
        ,MAX(IF(integration_type = 'Zoho',true, false)) as had_zoho
        ,MAX(IF(integration_type = 'Pipedrive', true, false)) as had_pipedrive
        ,MAX(IF(integration_type = 'API', true, false)) as had_api
From account_and_integrations
group by account_id
order by account_id*/