with account_info as (
        select account_id,
        country,
        currency,
        region_type from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
        where partner_discount='0.5')
        ,

discount_history as (
        select account_id,
        min(occured_date) as occured_date
        from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_billing_affiliate_subscription_payments_real_money_with_who_paid` 
        where partner_discount=0.5 and account_type in ('partner','tech-partner')
        group by 1
)
    -- Когда у партнера была первая оплата подписки со скидкой 50%
select discount_history.account_id as partner_id,   -- аккаунт партнера
        occured_date    -- дата, когда партнер в первый раз оплатил подписку в 50%
from discount_history 
inner join account_info 
on account_info.account_id=discount_history.account_id