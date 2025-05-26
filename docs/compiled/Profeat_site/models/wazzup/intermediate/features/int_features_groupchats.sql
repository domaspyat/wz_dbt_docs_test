-- Таблица c аккаунтами по месяцам, которые используют фичу групповых чатов
SELECT date_trunc(_ibk, month) as month, -- Месяц
cast(accountId as STRING) as account_id  -- ID аккаунта
FROM `dwh-wazzup`.`wazzup`.`messages_voice_groupchats` 
where chatType is not null
group by 1,2