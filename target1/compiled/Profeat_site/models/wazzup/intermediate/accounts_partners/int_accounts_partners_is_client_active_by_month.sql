with partners_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_with_clients_by_month`
),

subscriptions_period as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_active_accounts_by_month`
)

-- Какие аккаунты прикреплены к партнеру в этом месяце и у каких из них была активная оплаченная подписка (считаем, если был хотя бы один день). Не считаем триалы, обещанные платежи и бесплатные подписки
select distinct partner_id,     -- аккаунт партнера
partners_info.month,            -- рассматриваемый месяц
subscriptions_period.account_id as active_account_id,   -- у дочки есть оплаченная активная подписка
 partners_info.account_id as all_account_id             -- дочка прикреплена к партнеру
from partners_info
left join subscriptions_period on partners_info.account_id=subscriptions_period.account_id
and subscriptions_period.month=partners_info.month
where partner_type in ('partner','tech-partner')
and account_type='standart'