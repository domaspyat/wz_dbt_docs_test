with accounts_info as (
    Select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`),
specific_integrations as (
   select 
        account_id, 
                 (case when integration_type like '%amo%' then 'AMO'
                      when integration_type like  '%bitrix%' then 'Bitrix24'
                      when integration_type like  '%hubspot%' then 'Hubspot'
                      when integration_type like '%zoho%' then 'Zoho'
                      when integration_type like '%pipe%' then 'Pipedrive'  -- тип интеграции еще в разработке вроде
                      when integration_type like '%api%' then 'API'
                      when integration_type like '%planfix%' then 'Planfix'
                      when integration_type like '%megaplan%' then 'Megaplan'
                      when integration_type like '%salesforce%' then 'Salesforce'
                  end) as integration_type,

                  row_number() over (partition by account_id order by created_at desc) rn,
                  domain
   from `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_with_type_with_pipedrive`
   where state = 'active'
),
account_and_integrations as (
    select accounts_info.account_id,
           integration_type,
           domain
    from accounts_info
    left join specific_integrations on accounts_info.account_id = specific_integrations.account_id and rn=1
)   -- Таблица интеграций с группировкой по аккаунту
select account_id,  -- ID аккаунта
       max(if(integration_type is not null, integration_type,'does_not_have_an_active_integration')) active_integration_name,   -- Название активной интеграции
       max(if(integration_type is not null, domain,'does_not_have_an_active_integration')) domain                               -- Домен активной интеграции
From account_and_integrations
group by account_id