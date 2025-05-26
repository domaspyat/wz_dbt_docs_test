SELECT account_id,  -- ID аккаунта
max(case when action='renewal' and state='activated' and first_subscription.paid_at is not null then True else False end) as is_renewal     -- True, если подписка была продлена

FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_first_subscription_date_and_type`  first_subscription

left join `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` subscription_updates 
on first_subscription.subscription_id=subscription_updates.subscription_id
where first_subscription.period=1
group by 1
    -- Конверсия в повторную оплату месячной подписки