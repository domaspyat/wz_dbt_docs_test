with subscription_paid as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency`
    ),
profile_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
),
waba_revenue_postpay as (
    select account_id,
    paid_date as start_date,
    currency,
    amount as wapi_transactions_in_rubles
    from `dwh-wazzup`.`partners_info`.`postpay_waba_revenue`),

subscriptions_with_integration as (
   select  start_date,
    account_id,
   currency,
  subscription_type,
   sum(sum) as sum,
   sum(sum_in_rubles) as sum_in_rubles, 
   sum(original_sum) as original_sum,
   sum(sum_in_USD) as sum_in_USD,
   sum(coalesce(wapi_original_sum,0)) as wapi_original_sum,
   sum(coalesce(wapi_transactions_in_rubles,0)) as wapi_transactions_in_rubles,
   sum(coalesce(wapi_sum_in_USD,0)) as wapi_sum_in_USD
   from subscription_paid
   group by 1,2,3,4
) ,  


integraions_type as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_with_type_with_pipedrive`),

crm_marketplace as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_crmMarketplace`
    where status='published'
),    

payments_and_integrations as (
    select subscription_paid.start_date,
    subscription_paid.sum,
    subscription_paid.currency,
    subscription_paid.account_id,
    subscription_paid.subscription_type,
    partner_id,
    sum_in_rubles, 
    original_sum,
    sum_in_USD,
    wapi_original_sum,
    wapi_transactions_in_rubles,
    wapi_sum_in_USD,
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
    else integration_type_with_api
    end) as integration_type_with_api,
    integraions_type.integration_type,
    integraions_type.created_at
    from subscriptions_with_integration subscription_paid left join integraions_type
    on subscription_paid.start_date>=date_add(cast(integraions_type.created_at as date),interval -7 day) and subscription_paid.start_date<=integraions_type.integration_end_date
    and subscription_paid.account_id=integraions_type.account_id),

payments_and_integrations_to_deduplicate as (
  select *, row_number() over (partition by account_id,start_date, subscription_type, currency order by created_at desc) rn from payments_and_integrations 
),

payments_and_integrations_with_converted_sum as (
    select payments_and_integrations_to_deduplicate.start_date,             -- Дата оплаты подписки
    payments_and_integrations_to_deduplicate.sum,                           -- сумма (устарело)
    payments_and_integrations_to_deduplicate.currency,                      -- валюта оплаты
    payments_and_integrations_to_deduplicate.account_id,                    -- ID аккаунта
    payments_and_integrations_to_deduplicate.subscription_type,             -- тип подписки
    payments_and_integrations_to_deduplicate.sum_in_rubles,                 -- сумма в рублях
    payments_and_integrations_to_deduplicate.original_sum,                  -- сумма в валюте
    payments_and_integrations_to_deduplicate.sum_in_USD,                    -- сумма в долларах
    payments_and_integrations_to_deduplicate.wapi_original_sum,             -- траты на вабу в валюте
    payments_and_integrations_to_deduplicate.wapi_transactions_in_rubles,   -- траты на вабу в рублях
    payments_and_integrations_to_deduplicate.wapi_sum_in_USD,               -- траты на вабу в долларах
    partner_id,         -- ID партнера
    integration_type,   -- тип интеграции
    coalesce(crm_marketplace.crm_name,payments_and_integrations_to_deduplicate.integration_type_with_api) as integration_type_with_api, -- тип интеграции с детализацией интеграций api
    (case when status='published' then True 
    else False end
    ) as is_published   -- опубликована ли интеграция в маркетплейсе на данные момент
    from payments_and_integrations_to_deduplicate
    left join crm_marketplace
    on lower(crm_marketplace.crm_name)=lower(payments_and_integrations_to_deduplicate.integration_type_with_api)
    where rn=1
)
    -- Траты на подписки в зависимости от того, какая интеграция была в ЛК в момент оплаты
select payments_and_integrations_with_converted_sum.*
from payments_and_integrations_with_converted_sum
where not exists (
    select profile_info.account_id
    from profile_info 
    where   payments_and_integrations_with_converted_sum.account_Id = profile_info.account_Id
            and profile_info.is_employee 
)