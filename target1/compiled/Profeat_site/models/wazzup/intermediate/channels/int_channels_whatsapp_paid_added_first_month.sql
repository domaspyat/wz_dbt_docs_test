with
    profile_info as (select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`),

    channels_history as (
        select account_id, 
        date, 
        transport, 
        subscription_id,
        channel_id,
        is_free
        from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_channels`
    ),

    channels as (select guid, phone from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels`)
    -- Таблица c каналами WHATSAPP, которые были добавлены и оплачены в первый месяц после регистрации
select channels_history.account_id,             -- ID аккаунта
count(distinct channels.phone) as channel_count -- Количество каналов
from channels_history
inner join profile_info on profile_info.account_id = channels_history.account_id
inner join channels on channels.guid = channels_history.channel_id
where
    channels_history.date <= date_add(profile_info.register_date, interval 1 month)
    and channels_history.transport = 'whatsapp'
    and channels_history.subscription_id is not null and is_free is distinct from True
group by 1