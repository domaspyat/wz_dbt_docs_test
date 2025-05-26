with integrations as (
    select *, 
    row_number() over (partition by account_id, created_date order by created_at desc) as rn_created_on_day
     from `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_created_with_pipedrive`
),

integrations_deduplicated as (
    select * from integrations
    where rn_created_on_day=1
),

integrations_with_correct_end_date as (
    select *, lag(created_date,1,current_date) over (partition by account_id order by created_date desc)
    as next_integration_created_date
     from integrations_deduplicated

),

affiliates as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates`
),

tech_partner_info as (
    select * from `dwh-wazzup`.`analytics_tech`.`tech_partner_info_account_crm_marketplace_bq`
),

crm_marketplace as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_crmMarketplace`
),

integration_types as (
    select  integration_type, 
    api_type_field,  
    integrations.created_at,
    (case when integrations.integration_end_date>=next_integration_created_date 
    then next_integration_created_date
    else integration_end_date
    end) as integration_end_date,
    integrations.account_id,
    state,
	(case 
    when  integration_type not like '%api%' then integration_type
    when marketplace_type is not null then marketplace_type
    when crm_marketplace.crm_name is not null then  crm_marketplace.crm_name
    when integration_type not like '%api%' then integration_type
    when integrations.crm_name is not null then integrations.crm_name
    when api_type_field like '%myenvy%' then 'EnvyCRM'	
    when api_type_field like '%s20%' then 'alfacrm_v3'
    when api_type_field like '%apimonster%' then 'Api Monster'
    when api_type_field like '%oasis38%' then 'oasisCRM'
    when api_type_field like '%cbox%' then 'cboxCRM'
    when api_type_field like '%clientbase%' then 'clientbase'
    when api_type_field like '%envybox%' then 'EnvyCRM'
    when api_type_field like '%ngrok%' then 'megaplan'
    when api_type_field like '%brizo%' then 'brizoCRM'
    when api_type_field like '%brizohooks%' then 'brizoCRM'
    when api_type_field like '%clientix%' then 'klientiks'
    when api_type_field like '%doktor365%' then 'doktor365'
    when tech_partner_info.crm_name='JokerCRM' then 'JokerCRM'
    when api_type_field is null then 'api'
    when api_type_field like '%fitbase%' then 'fitbase'
    when (REGEXP_EXTRACT(api_type_field,  r'([a-z0-9-]*)\.[a-z]*\/'))  is null then 'api'
    else REGEXP_EXTRACT(api_type_field,  r'([a-z0-9-]*)\.[a-z]*\/')
    END) as integration_type_api,
    coalesce(crm_marketplace.crm_name,coalesce(tech_partner_info.crm_name, marketplace_refparent.crm_name)) as marketplace_name, 
    domain,
    --приоритетет названию из нашей базке. если его нет, смотрим название из файлика отдела продаж
    tech_partner_info.isMarketplace as is_marketplace,
    partner_id,
    from integrations_with_correct_end_date integrations
    left join affiliates on cast(affiliates.child_id as string)=cast(integrations.account_id as string)
    left join tech_partner_info on tech_partner_info.account_id=cast(affiliates.partner_id as string)
    left join tech_partner_info marketplace_refparent on marketplace_refparent.account_id=cast(affiliates.refparent_id as string)
    left join crm_marketplace on crm_marketplace.account_id=affiliates.partner_id),

inegrations_with_marketplace_name as (
select integration_type,            -- Тип соединения
    integration_types.account_id,   -- ID аккаунта
    created_at,                     -- Дата и время создания интеграции
    integration_end_date,           -- Дата окончания действия интеграции
    partner_id,                     -- ID аккаунта партнера
    state,                          -- Состояние интеграции
     (
    case when integration_type not like '%api%' then  coalesce(crm_marketplace.crm_name,integration_type)
    when crm_marketplace.crm_name is not null then crm_marketplace.crm_name
    when integration_type_api!='api' then coalesce(integration_types.marketplace_name, integration_type_api)
    when marketplace_name is not null then marketplace_name
    else 'api'
    end
    ) as integration_type_with_api, -- Название интеграции
    domain                          -- Домен интеграции
    from integration_types
    left join crm_marketplace on LOWER(integration_types.integration_type_api)=lower(crm_marketplace.crm_code)
    and status='published'),

    integration_with_dates as (

select *,
cast(created_at as date ) as integration_start_date,                -- Дата начала действия интеграции
date_trunc(created_at, month) as integration_start_month,           -- Месяц начала действия интеграции
date_trunc(integration_end_date, month) as integration_end_month    -- Месяц окончания действия интеграции
 from inegrations_with_marketplace_name)
    -- Таблица, которая показывает тип интеграции api у аккаунта, учитывая pipedrive
 select * from integration_with_dates