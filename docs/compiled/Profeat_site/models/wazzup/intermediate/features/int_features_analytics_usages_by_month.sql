-- Таблица c аккаунтами по месяцам, которые используют фичу аналитики
SELECT date_trunc(date, month) as month,  -- Месяц
cast(accountId as STRING) as account_id   -- ID аккаунта
FROM `dwh-wazzup`.`yandex_metrika`.`wazzup_hits_analytics` 
where accountId is not null and accountId!='nan'
group by 1,2
having count(*)>=2