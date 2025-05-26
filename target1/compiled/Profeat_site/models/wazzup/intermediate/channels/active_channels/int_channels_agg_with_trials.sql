
with channels_agg_deduplicated as (
    select 
    account_id,
    package_id,
    channel_id,
    transport,
    min_datetime,
    max_datetime
    from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_agg_deduplicated`
),

accounts as (
    select 
    account_id,
    whatsap_trial, 
    instagram_trial,
    tgapi_trial,
    wapi_trial,
    avito_trial,
    vk_trial,
    telegram_trial
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
),

billing_packages as (
    select guid,
    is_free
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`
),

channels_with_trials as (
    select package_id,  -- ID подписки
    channels_agg_deduplicated.account_id,   -- ID аккаунта
    channel_id,         -- ID канала
    transport,          -- Транспорт канала
    min_datetime,       -- Минимальная дата и время изменения
    cast(min_datetime as date) as min_date, -- Минимальная дата изменения
    max_datetime,       -- Максимальная дата и время изменения
    cast(max_datetime as date) as max_date, -- Максимальная дата изменения
    is_free,            -- Это бесплатный канал?
    whatsap_trial,      -- Дата и время окончания WHATSAPP триала
    instagram_trial,    -- Дата и время окончания INSTAGRAM триала
    tgapi_trial,        -- Дата и время окончания TGAPI триала
    wapi_trial,         -- Дата и время окончания WABA триала
    avito_trial,        -- Дата и время окончания AVITO триала
    vk_trial,           -- Дата и время окончания VK триала
    telegram_trial,     -- Дата и время окончания TELEGRAM триала
    cast((case when transport='whatsapp' then whatsap_trial
    when transport='instagram' then instagram_trial
    when transport='avito' then avito_trial
    when transport='tgapi' then tgapi_trial
    when transport='waba' then wapi_trial
    when transport='avito' then avito_trial
    when transport='vk' then vk_trial
    when transport='telegram' then telegram_trial
    end) as date) as trial_end_date                 -- Дата окончания триала
    from channels_agg_deduplicated left join billing_packages
    on billing_packages.guid=channels_agg_deduplicated.package_Id
    left join accounts on accounts.account_id=channels_agg_deduplicated.account_id)
    -- Таблица c активными каналами после группировки с триалами
select * from  channels_with_trials