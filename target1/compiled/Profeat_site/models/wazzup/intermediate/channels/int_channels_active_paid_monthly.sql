with combined_intervals as (
    select int_subscription_deduplicated.*,
            billingpackages.type
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_deduplicated_by_id_combined_intervals` int_subscription_deduplicated
    inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingpackages 
            on billingpackages.guid=int_subscription_deduplicated.subscription_id
    where billingpackages.paid_at is not null 
  -- and int_subscription_deduplicated.account_id =  59580380
   order by subscription_end
),

first_paid_date as (
    select account_id, min(subscription_start) as min_start_date from combined_intervals
    group by 1
),


subscription_with_months  as (
    select distinct
    month,
    subscription_id, 
    subscription_start,
    account_id,
    LAST_DAY(month, MONTH)  as last_day_of_month
    from combined_intervals inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_months` months
    on months.month>=date_trunc(combined_intervals.subscription_start,month) and  ((LAST_DAY(months.month, MONTH)<=combined_intervals.subscription_end)
    or (combined_intervals.subscription_end>=current_date()))
    
),

last_value_tarif as (
    select stg_subscriptionUpdates.subscription_id,
        --last_value(coalesce(partner_account_id,billingpackages.account_id)) over (partition by billingpackages.account_id, date_trunc(stg_subscriptionUpdates.paid_date, month) order by stg_subscriptionUpdates.created_at asc rows between unbounded preceding and unbounded following) as who_paid_account_id,
            coalesce(partner_account_id,billingpackages.account_id) who_paid_account_id,
    date_trunc(stg_subscriptionUpdates.paid_date, month) as paid_month,
    billingpackages.type as subscription_type,
    billingpackages.account_id as subscription_owner_account_id,
    last_value(tariff_new) over (partition by stg_subscriptionUpdates.subscription_id, date_trunc(stg_subscriptionUpdates.paid_date, month) order by stg_subscriptionUpdates.created_at asc rows between unbounded preceding and unbounded following) as tariff_new,
    last_value(period_new) over (partition by stg_subscriptionUpdates.subscription_id, date_trunc(stg_subscriptionUpdates.paid_date, month) order by stg_subscriptionUpdates.created_at asc rows between unbounded preceding and unbounded following) as period_new,
    last_value(quantity_new) over (partition by stg_subscriptionUpdates.subscription_id, date_trunc(stg_subscriptionUpdates.paid_date, month) order by stg_subscriptionUpdates.created_at asc rows between unbounded preceding and unbounded following) as quantity_new
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_updates__corect_date_and_filledna_tarif_period_quantity` stg_subscriptionUpdates
   -- join  `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_and_wapi_sessions_real_money_with_data_source_and_subscription_update_id` who_paid_data 
                                                                                                  --  on stg_subscriptionUpdates.guid = who_paid_data.subscription_update_id
    left join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`  billingpackages 
    on billingpackages.guid=stg_subscriptionUpdates.subscription_id
    where tariff_new is not null
    --and billingPackages.account_Id = 82596703
),

last_value_tarif_by_subscription_id as (
    select subscription_id,
    subscription_type,
    subscription_owner_account_id,
    who_paid_account_id,
    paid_month, 
    tariff_new,
    period_new,
    quantity_new 
    from last_value_tarif
    where tariff_new is not null 
    group by 1,2,3,4,5,6,7,8
),

last_value_tarif_next_month as ( 
    select *, coalesce(lag(paid_month) over (partition by subscription_id order by paid_month desc), date_add(current_date, interval 1 month)) as next_month
    from last_value_tarif_by_subscription_id
),tarif_info as (
    select subscription_with_months.*, 
    tariff_new,
    period_new,
    quantity_new,
    subscription_type,
    who_paid_account_id,
    subscription_owner_account_id
     from subscription_with_months
    left join last_value_tarif_next_month
    on last_value_tarif_next_month.subscription_id=subscription_with_months.subscription_id

    and subscription_with_months.month>=last_value_tarif_next_month.paid_month and 
    subscription_with_months.month<last_value_tarif_next_month.next_month
    --where subscription_owner_account_id = 51109365
    ),
mart_active_accounts_by_date_to_deduplicate as (
  select *,
  row_number() over (partition by account_id, date order by subscription_start desc) as rn_segment
   from   `dwh-wazzup`.`dbt_nbespalov`.`mart_active_accounts_by_days_by_segment`
   --where account_id = 56983405
 ), active_accounts_by_date_deduplicated as (
    select *,  
    last_value(segment ignore nulls) over (partition by account_id, date_trunc(date,month) order by date asc rows between unbounded preceding and unbounded following) as last_value_segment_month,
    date_trunc(date,month) as segment_month
     from mart_active_accounts_by_date_to_deduplicate
    where rn_segment=1
), segments_monthly as (
    select distinct account_id,
                    segment_month,
                    last_value_segment_month
    from active_accounts_by_date_deduplicated
),segments_joint as (
select tarif_info.*except(account_Id,who_paid_account_id), case when last_value_segment_month in ('of_partner_child__of_partner_paid','partner') then 'of-partner'
                      when last_value_segment_month in ('tech_partner_child__child_paid','tech_partner_child__tech_partner_paid','tech-partner','tech-partner-postpay') then 'tech-partner'
                    when last_value_segment_month in ('standart_without_partner','of_partner_child_child_paid','unknown') then 'final_client'
                      end segment_aggregated,
                      last_value_segment_month,
            last_value(who_paid_account_id) over (partition by month,subscription_owner_account_id order by subscription_start asc rows between unbounded preceding and unbounded following) as who_paid_account_id,

from tarif_info
join segments_monthly on tarif_info.subscription_owner_account_id = segments_monthly.account_id
                              and tarif_info.month = segments_monthly.segment_month
                             and last_value_segment_month not in ('employee')
where month <= date_trunc(current_date(),month)


)   -- Таблица c активными и оплаченными каналами
 select who_paid_account_id as account_id,                          -- ID аккаунта
            subscription_type,                                      -- Тип (транспорт) подписки
            subscription_owner_account_id,                          -- ID аккаунта владельца подписки
            segment_aggregated as segments_aggregated,              -- Сегмент клиента после группировки
              month,                                                -- Месяц между началом подписки и её окончанием
              date_trunc(register_date,month) registration_month,   -- Месяц регистрации клиента
              currency,                                             -- Валюта
  sum(quantity_new) as paid_channels_quantity,                      -- Количество оплаченных каналов
   sum(case when subscription_type in ('waba','wapi') then quantity_new end) as paid_channels_waba_quantity,    -- Количество оплаченных каналов с тарифом WABA
   sum(case when tariff_new='pro' then quantity_new end) as paid_channels_pro_quantity,                         -- Количество оплаченных каналов с тарифом PRO
    sum(case when tariff_new  in ('max','waba') then quantity_new end) as paid_channels_max_quantity,           -- Количество оплаченных каналов с тарифом MAX
    sum(case when tariff_new='start' then quantity_new end) as paid_channels_start_quantity                     -- Количество оплаченных каналов с тарифом START
  from segments_joint
  join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accs on segments_joint.who_paid_account_id = accs.account_id
  group by who_paid_account_id,
            subscription_owner_account_id,
            subscription_type,
             segment_aggregated,
              month,
              date_trunc(register_date,month),
              currency