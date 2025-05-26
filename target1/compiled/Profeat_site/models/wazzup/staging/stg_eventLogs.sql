select  -- Таблица с отображением всех возможных изменений по пользователю
        accountId as account_id, -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
        userid as user_id,  -- Идентификатор пользователя, соответствует guid из таблицы users
        subjectType as subject_type,    -- 3 - для всех событий интеграций, 10 - для всех событий с подпиской
        subjectId as subject_id,    -- объект, с которым происходит изменения. в случае интеграций - guid интеграци из integrations, в случае подписки guid из billingPackages
        logType as log_type,    -- api_v1_webhook, billingPackages - изменения , связанные с подпиской, save_options, amo_unsorted_del, send_api_v2-like_webhook_error, before_conv_opts_backup, integrationWarning, integrationError, amo_unsorted, integrationReconnected, amo_chat_api_msg
        level,
        datetime(datetime,'Europe/Moscow') as occured_at,   -- Дата и время события, формат 2022-11-29T19:49:52.778Z
        state,  -- Актуально для log_type = billingPackages. Указывает состояние подписки после изменения. active - активная; expired - истекла; deleted - удалена
        datetime(paidat,'Europe/Moscow') as paid_at,    -- Дата и время оплаты подписки, формат 2022-11-29T19:49:52.778Z
        datetime(date_add(expiresAt,interval accounts.time_zone hour)) as expires_at,   -- Дата и время истечения срока подписки, формат 2022-11-29T19:49:52.778Z
        _ibk,   -- Дата, формат 2022-11-29. Совпадает с полем occured_at::date. _ibk необходимо для партицирования данных в BigQuery
        eventLogs.details,  -- Детали происходящего изменения
        id, -- Идентификатор записи
        datetime(date_add(promisedPaymentStartDate,interval accounts.time_zone hour)) as promised_payment_start_date,   -- Дата и время начала действия обещанного платежа, формат: 2022-11-29T19:49:52.778Z
        cast( datetime(date_add(promisedPaymentStartDate,interval accounts.time_zone hour)) as date) as promised_payment_start,
        datetime(date_add(promisedPaymentEndDate,interval accounts.time_zone hour)) as promised_payment_end_date,   -- Дата и время конца действия обещанного платежа, формат: 2022-11-29T19:49:52.778Z
        cast(datetime(date_add(promisedPaymentEndDate,interval accounts.time_zone hour)) as date) as promised_payment_end,
        autoRenewal,    -- Включено автопродление?
        case when promisedpaymentstartdate is not null then promisedpaymentstartdate    
             when paidAt is not null and expiresat is not null and state='deleted' then null
             when paidAt is not null and expiresat is not null then paidat
             when promisedpaymentenddate is not null then dateTime
        end as start_time,  -- Если дата обещанного платежа is not null, то дата обещанного платежа; если есть дата оплаты и дата истечения срока подписки И статус = 'deleted', то null ; если есть дата оплаты и дата истечения подписки, то дата оплаты подписки ; если есть дата окончания действия обещанного платежа, то дата окончания действия обещанного платежа, иначе null.

        case when  (state!='deleted' or state is null) and promisedpaymentenddate is not null and expiresat>=promisedpaymentenddate then expiresAt 
             when  (state!='deleted' or state is null) and promisedpaymentenddate is  null then expiresat 
             when promisedpaymentenddate is not null then promisedpaymentenddate
        end as subscription_end,    -- Дата и время окончания подписки, формат: 2022-11-29T19:49:52.778Z

        case  when state not in ('deleted') or state is null  
              then 'subscription'
              else 'deleted' 
        end as event,      -- Событие 
        promisedPaymentType as promised_payment_type    -- Тип обещанного платежа (повышение тарифа, период)
from `dwh-wazzup`.`wazzup`.`eventLogs` eventLogs
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts
on eventLogs.accountId=accounts.account_id