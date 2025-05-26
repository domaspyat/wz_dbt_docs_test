with defining_first_rn as (
   select id,
         row_number() over (partition by subscription_id order by cast(id as int)) rn
from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_transactions_gupshup` wgt
join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` bp  on wgt.subscription_id = bp.guid
where guid  in (select distinct subscription_id from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_transactions` where subscription_id is not null )
)

select cast(child_Id as string) as account_id, 
   int_acccounts__type_change_deduplicated.type as account_type,
   cast(partner_Id as string) as partner_id, 
   'tech-partner-postpay' as partner_type,
   wabaTransactions.currency, 
   cast(DATETIME(date_at,'Europe/Moscow') as date) as paid_date,
   amount
from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_transactions_gupshup`  wabaTransactions
left join defining_first_rn on wabaTransactions.id = cast(defining_first_rn.id as int)  and rn = 1
inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingPackages
                on billingPackages.guid=wabaTransactions.subscription_id
inner join `dwh-wazzup`.`dbt_nbespalov`.`int_acccounts__type_change_deduplicated` on billingPackages.account_id = int_acccounts__type_change_deduplicated.account_id
                                                         and DATETIME(date_at,'Europe/Moscow') >= start_occured_at and DATETIME(date_at,'Europe/Moscow') < end_occured_at
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates` affilaites 
          on affilaites.child_Id= billingPackages.account_id
where int_acccounts__type_change_deduplicated.type='child-postpay'
      and partner_Id!=96955135
   and wabaTransactions.type='topup'
   and defining_first_rn.id is null



   union all
   select 
   cast(child_Id as string) as account_id, 
   int_acccounts__type_change_deduplicated.type as account_type,
   cast(partner_Id as string) as partner_id, 
   'tech-partner-postpay' as partner_type,
   wabaTransactions.currency, 
   cast(DATETIME(date_at,'Europe/Moscow') as date) as paid_date,
   amount
from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_transactions`  wabaTransactions
inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingPackages
                on billingPackages.guid=wabaTransactions.subscription_id
inner join `dwh-wazzup`.`dbt_nbespalov`.`int_acccounts__type_change_deduplicated` on billingPackages.account_id = int_acccounts__type_change_deduplicated.account_id
                                                         and DATETIME(date_at,'Europe/Moscow') >= start_occured_at and DATETIME(date_at,'Europe/Moscow') < end_occured_at
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates` affilaites 
          on affilaites.child_Id= billingPackages.account_id
where int_acccounts__type_change_deduplicated.type='child-postpay'
      and partner_Id!=96955135
   and wabaTransactions.type='topup'