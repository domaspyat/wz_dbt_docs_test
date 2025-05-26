with subscription_information as (
  select subject_id,        -- объект, с которым происходит изменения. в случае интеграций - guid интеграци из integrations, в случае подписки guid из billingPackages
         occured_at,        -- Дата и время события, формат 2022-11-29T19:49:52.778Z
         start_time,        -- Если дата обещанного платежа is not null, то дата обещанного платежа; если есть дата оплаты и дата истечения срока подписки И статус = 'deleted', то null ; если есть дата оплаты и дата истечения подписки, то дата оплаты подписки ; если есть дата окончания действия обещанного платежа, то дата окончания действия обещанного платежа, иначе null
         subscription_end,  -- Дата и время окончания подписки, формат: 2022-11-29T19:49:52.778Z
         event,             -- Событие
         account_id         -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
  from `dwh-wazzup`.`dbt_nbespalov`.`stg_eventLogs`
  where log_type='billingPackages' 
        and (state not in ('expired') or state is null)
    )
        -- Таблица с детальной информацией о подписке с данными из eventlogs
select subscription_information.*,
        billingPackages.* except(account_Id),
    case when lag(event,1)  OVER (partition by subject_Id ORDER BY occured_at DESC)='deleted' then LAG(subscription_end,1) OVER (partition by subject_Id ORDER BY occured_at DESC)
    end deleted_datetime,
    type as subscription_type
from  subscription_information
join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingPackages
        on billingPackages.guid=subscription_information.subject_Id