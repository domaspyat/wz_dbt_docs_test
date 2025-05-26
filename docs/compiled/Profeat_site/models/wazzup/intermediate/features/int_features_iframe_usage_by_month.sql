with iframe as (SELECT date_trunc(date, month) as month,  -- Месяц
cast(accountId as STRING) as account_id,            -- ID аккаунта
count(distinct clientID) as iframe_open_employees   -- Количество сотрудников, которые открывали iFrame (iFrame - это наше окно чатов Wazzup)
 FROM 
`dwh-wazzup`.`yandex_metrika`.`wazzup_hits_iframe` 
where accountId is not null and accountId!='nan'
group by 1,2),

subscriptions as (
    select cast(account_id as string) as account_id, 
    subscription_start,
    subscription_end
     from 
    `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicated_without_promised_date_combined_intervals`
)
-- Таблица c аккаунтами по месяцам, которые используют фичу iFrame
select iframe.* from iframe 
inner join subscriptions
on iframe.account_id=subscriptions.account_id
and iframe.month>=date_trunc(subscriptions.subscription_start,month)
and iframe.month<=date_trunc(subscriptions.subscription_end,month)