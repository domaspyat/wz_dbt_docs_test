-- Таблица c аккаунтами по месяцам, которые используют фичу messages_voice_groupchats
SELECT date_trunc(_ibk, month) as month, -- Месяц
cast(accountId as STRING) as account_id  -- ID аккаунта   
FROM `dwh-wazzup`.`wazzup`.`messages_voice_groupchats` 
where isstt=True
group by 1,2
having count(*)>=4