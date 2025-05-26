with amount_balance_by_month as (
select date_trunc(waba_transactions.transaction_date, month) as paid_month, sum(amount*coalesce(RUR,1)) as sum_in_rubles
from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_transactions` waba_transactions
left join  `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_sessions` waba_sessions
on waba_sessions.transaction_id = waba_transactions.id
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billing_packages on billing_packages.guid=waba_transactions.subscription_id
left join   `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates 
on exchange_rates.currency=waba_transactions.currency and exchange_rates.data=waba_transactions.transaction_date
where waba_sessions.state is distinct from 'canceled'
and amount!=0 
and subscription_id is distinct from '57bf9315-afcb-4421-a18f-b053097dec27'
and not exists 
(select account_id 
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` partner_type_and_account_type 
where billing_packages.account_id=partner_type_and_account_type.account_id and account_type='employee') 
group by 1
)
    -- Таблица с общим балансом WABA по месяцам
select *, sum(sum_in_rubles) over (order by paid_month asc) balance from amount_balance_by_month