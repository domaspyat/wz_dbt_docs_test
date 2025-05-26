with integrations_min_max_date as 

(select * from `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_min_max_date`),

generated_periods as (
    select Period
    from integrations_min_max_date t 
    join UNNEST(GENERATE_DATE_ARRAY(t.min_date, t.max_date)) period
),

specific_integrations as (
   select 
        account_id, 
        case when integration_type like '%amo%' then 'AMO'
             when integration_type like  '%bitrix%' then 'Bitrix24'
             when integration_type like  '%hubspot%' then 'Hubspot'
             when integration_type like '%zoho%' then 'Zoho'
             when integration_type like '%pipe%' then 'Pipedrive'  -- тип интеграции еще в разработке вроде
             when integration_type like '%api%' then 'API'
             end as integration_type,
        created_date,
        integration_end_date
   from `dwh-wazzup`.`dbt_nbespalov`.`stg_integrations`
   where    integration_type like '%amo%'    
             or integration_type like  '%bitrix%' 
             or integration_type like  '%hubspot%'
             or integration_type like '%zoho%'  
             or integration_type like '%pipe%'  
             or integration_type like '%api%')

    select *
    from specific_integrations
    left join generated_periods ii   
        on ii.period >= cast(created_date as date)
           and ii.period <= integration_end_date