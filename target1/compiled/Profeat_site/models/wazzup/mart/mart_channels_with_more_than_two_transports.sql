with channels as (
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_channels_transport_with_jinja`
),
profile_info as (
    select  * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
)
    -- Таблица с информацией о количество каналов каждого аккаунта в разбивке по транспортам
select channels.*,
profile_info.type,                  -- Тип аккаунта
profile_info.russian_country_name,  -- Название страны на русском языке
profile_info.account_segment_type,  -- Сегмент пользователя
profile_info.region_international,  -- регион пользователя (СНГ, МЕНА, ЛАТАН ) согласно нашему внутреннему разделению
profile_info.first_paid_subscription_date_with_1_more_day_duration, -- дата первой оплаты подписки, если она длилалась более 1 дня (не была удалена в тот же день), формат 2022-01-09
profile_info.register_date,         -- Дата регистрации
profile_info.currency               -- Валюта

 from channels left join profile_info 
on channels.account_id=profile_info.account_id