with subscriptions_without_trials_and_with_promised_payments_and_payments as (
    select distinct account_id
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicated_with_payments_and_promised_payments_combined_intervals`
    where subscription_end >= current_date()
 )  -- Таблица аккаунтов, у которых есть любая активная подписка (бесплатные подписки и обещанные платежи учитываются)
 select distinct account_Id     -- ID аккаунта
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingPackages 
    where (state = 'active' and paid_At is not null and is_free is distinct from True)
      or (is_free and type in ('vk','telegram') and state = 'active')
      or exists (select subscriptions_without_trials_and_with_promised_payments_and_payments.account_Id 
                 from   subscriptions_without_trials_and_with_promised_payments_and_payments
                 where subscriptions_without_trials_and_with_promised_payments_and_payments.account_id =  billingPackages.account_id
                    )