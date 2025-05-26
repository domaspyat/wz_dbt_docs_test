with profile_info as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
),
gupshup_channels as (
    select distinct phone
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels` 
    where transport in ('wapi','waba')
                and deleted = False
                and is_gupshup_waba is True
                and temporary = False
),
channels as (
    select distinct stg_channels.*
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels` 
    left join gupshup_channels on stg_channels.phone = gupshup_channels.phone
    where transport in ('wapi','waba')
                and deleted = False
                and is_gupshup_waba is false
                and temporary = False
                and gupshup_channels.phone is null
    )
, waba_channels_details as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_waba_channels_details`
),
subscription_info as (
    select account_id, 
        max(subscription_end) as last_subscription_end
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_left_and_returned_date_with_account_type_and_partner_type`
    group by account_id
),
telegram_notifications as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_account_administrators_and_telegram_usernames`
)   -- Таблица с указанием каналов, которые до сих пор находятся у KeyReply. Тут только ваба каналы
select  channels.guid as channels_guid,                 -- Идентификатор канала, соответсвует guid из stg_channels
        channels.phone as channels_number,              -- Номер телефона, на котором создана ваба
        channels.created_date as channel_creation_date, -- Дата создания канала
        profile_info.email,                             -- почта пользователя, указанная при регистрации
        profile_info.phone as accounts_phone_number,    -- телефон пользователя, указанный при регистрации
        telegram_username,                              -- Телеграм администраторов уведомлений - телеграм пользователей, которым приходят уведомления
        channels.account_Id,                            -- ID аккаунта
        channels.state,                                 -- Состояние канала
        tier,                                           -- Тир канала. https://wazzup24.atlassian.net/wiki/spaces/WAZ/pages/2561854/WABA#%D0%A3%D0%B2%D0%B5%D0%BB%D0%B8%D1%87%D0%B5%D0%BD%D0%B8%D0%B5-TIER
        profile_info.account_language,                  -- язык ЛК пользователя, указанный на текущий момент
        profile_info.partner_id,                        -- ID партнера
        partners_accounts.email as partner_accounts_email,  -- почта партнра, указанная при регистрации
        partners_accounts.account_language as partners_accounts_account_language,   -- язык ЛК партнера, указанный на текущий момент
        case when last_subscription_end is null then 'never_had_paid_subscription'
             when last_subscription_end >= current_date() then 'still_active'
             when last_subscription_end < current_date() then cast(FORMAT_DATE('%d-%m-%Y',PARSE_DATE('%Y-%m-%d',cast(last_subscription_end as string))) as string)
             end  as subsrciption_status                -- Статус платной подписки клиента (без учета триалов и обещанных платежей). Подробнее в доке
from channels
join profile_info on channels.account_id = profile_info.account_Id
left join waba_channels_details channel_details on channels.guid = channel_details.channel_id
left join telegram_notifications on channels.account_id = telegram_notifications.account_id 
left join subscription_info on channels.account_Id = subscription_info.account_Id
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates` stg_affiliates on stg_affiliates.child_id=profile_info.account_id
left join profile_info partners_accounts on stg_affiliates.partner_id=partners_accounts.account_id
where profile_info.type not in ('child-postpay','tech-partner-postpay')
and profile_info.is_employee is false