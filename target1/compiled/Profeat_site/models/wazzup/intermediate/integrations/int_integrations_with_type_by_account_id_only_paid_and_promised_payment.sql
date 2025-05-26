with paid_subscription_with_promised_payments as (
    select account_id,          -- ID аккаунта
    subscription_start,         -- Дата начала подписки
    (case when subscription_end>=current_date
    then current_date
    else subscription_end
    end) as subscription_end    -- Дата окончания подписки
     from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicated_combined_intervals`
),

days as (
  select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_days`
),

subscriptions_with_days as (
  select paid_subscription_with_promised_payments.*,
  date from paid_subscription_with_promised_payments    -- Дата между началом подписки и её окончанием
  inner join days
  on subscription_start<=days.date
  and days.date<= subscription_end
),

integration_with_type_days as (
    select * from 
    `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_with_type_with_pipedrive`  int_integrations_with_type_with_pipedrive
    inner join days
  on int_integrations_with_type_with_pipedrive.integration_start_date<=days.date
  and days.date<= int_integrations_with_type_with_pipedrive.integration_end_date
),

integration_with_type_subscription_days as (
  select subscriptions_with_days.*,
  integration_type_with_api,    -- Название интеграции
  integration_type,             -- Тип соединения
  integration_start_date,       -- Дата начала действия интеграции
  integration_end_date          -- Дата окончания действия интеграции
   from subscriptions_with_days
  left join integration_with_type_days
  on integration_with_type_days.date=subscriptions_with_days.date
  and integration_with_type_days.account_id=subscriptions_with_days.account_id
)


select *,   -- Таблица, которая показывает тип интеграции api у аккаунта (только с оплаченной подпиской или обещанным платежом)
row_number() over (partition by account_id, date order by integration_start_date desc) as rn,       -- Берутся только последние созданные интеграции
DATE_TRUNC(cast(subscription_start as DATE), MONTH) as integration_paid_period_start_date_month,    -- Месяц начала оплаты
 DATE_TRUNC(cast(subscription_end as DATE), MONTH) as integration_paid_period_end_date_month        -- Месяц окончания оплаты
 from integration_with_type_subscription_days