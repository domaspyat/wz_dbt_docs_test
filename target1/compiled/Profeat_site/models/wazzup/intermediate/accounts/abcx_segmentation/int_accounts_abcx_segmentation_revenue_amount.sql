with int_subscriptions_subscription_with_sum_and_converted_currency_billing_date as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency_billing_date`
)

,stg_billingPackages as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`
)

,int_accounts_who_paid__standart_russian_users_without_partners_living_time as (
    select distinct account_id
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_abcx_segmentation_users_living_time`
), revenue_amount as (
select date_trunc(paid_at_billing_date,month) as month,
          subs_updates.account_id,
          coalesce(period_new,period) period,
          sum(sum_in_rubles-wapi_transactions_in_rubles) as sum_in_rubles_all
          from int_subscriptions_subscription_with_sum_and_converted_currency_billing_date subs_updates
inner join stg_billingPackages billingPackes on billingPackes.guid=subs_updates.subscription_id
inner join int_accounts_who_paid__standart_russian_users_without_partners_living_time accounts_live_time on  billingPackes.account_id = accounts_live_time.account_id 

where  sum !=0

group by date_trunc(paid_at_billing_date,month), subs_updates.account_id,period

union all 

select  
        date_trunc(_ibk,month) as month,
          accountid,
          cast(period as int) period,
          sum(cost)*40 as sum_in_rubles_all
          from wazzup.billing billing
inner join int_accounts_who_paid__standart_russian_users_without_partners_living_time accounts_live_time on  billing.accountid = accounts_live_time.account_id 
where object='package'
group by date_trunc(_ibk,month),accountid,cast(period as int)),
combined_revenue_amount as (
select month,           -- Месяц, сгенерированный на основе дате выручки и expired_days, формат 2022-11-29
        account_id,     -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
        period,         -- Период подписки
        sum(sum_in_rubles_all) sum_in_rubles_all    -- Стоимость подписки в рублях
from revenue_amount
group by month,
        account_id,
        period)
select *,   -- Таблица с выручкой от пользователя за подписки
    -- row_number() over (partition by combined_revenue_amount.account_id,live_month order by start_month desc)
from combined_revenue_amount
where sum_in_rubles_all != 0
                                                                                                  --and combined_revenue_amount.month = accounts_live_time.live_month
                                                                                                  --and combined_revenue_amount.month = accounts_live_time.start_month