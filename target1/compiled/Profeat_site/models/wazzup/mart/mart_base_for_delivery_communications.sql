with accounts_info as (
    Select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` 
),

integrations as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_groupped_by_accounts_type`
),

not_deleted_and_errorless_integrations as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_not_deleted_and_error_groupped_by_accounts_type`
),
active_integrations as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_active_groupped_by_accounts_type`
),

all_channels as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_all_groupped_by_accounts_transport`
),
count_channels as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_count_by_account`
),
count_active_channels as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_count_active_by_account`
),
int_accounts_minimum_trial_start_dates_situation as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_minimum_trial_start_dates_situation`
),
paid_accounts as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_paid_subscription_with_type_and_tariff`
),
active_with_free_and_promised_payments_and_without_trials as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_with_active_subscriptions_paid_promised_free`
)
    /* Таблица со списком всех наших пользователей (за исключением работников, дочек постоплатников и демо-аккаунты партнеров) 
     и информацией об их интеграциях/каналах/платности. Нужна в основном для проведения маркетинговых коммуникаций или различных анализов, где требуется выгрузка разного сегмента пользователей */
Select accounts_info.account_Id,                    -- ID аккаунта
        email,                                      -- почта пользователя, указанная при регистрации
        name,                                       -- имя пользователя, указанное при регистрации
        phone,                                      -- телефон пользователя, указанный при регистрации
        channels_in_package,                        -- Количество до конца созданных каналов (с привязанным номером телефона), которые не удалены, и у которых есть активная бесплатная или активная платная подписка на данный момент
        channels_count,                             -- Количество до конца созданных каналов (с привязанным номером телефона), которые не удалены
        country,                                    -- страна пользователя, как мы её определили в формате alpha-2
        currency,                                   -- Валюта
        account_language,                           -- язык ЛК пользователя, указанный на текущий момент
        account_leaving_situation_type,             -- Причина ухода пользователя. Описано в доке
        account_segment_type,                       -- Сегмент пользователя
        partner_id,                                 -- ID партнера
        refparent_id,                               -- ID реф. партнера
        register_date,                              -- Дата регистрации
        integrations.* except(account_Id),
        active_integrations.* except(account_Id),
        all_channels.* except(account_Id),
        not_deleted_and_errorless_integrations.* except(account_id),                    -- Тип последней созданной интеграции, которая на данный момент не в статусе ошибки или удалена. Если 'does_not_have_a_not_deleted_and_errorless_integration', значит у пользователя нет ни одной неудаленной/не в статусе ошибки интеграции на данный момент
        trial_max_date_at_the_moment,               -- Последний активный день триала на данный момент в формате даты
        accounts_info.russian_country_name,         -- Название страны на русском языке
        accounts_info.region_international,         -- Регион
        coalesce(trial_max_day,'did_not_have_trial') as trial_max_day,                  -- Последний активный день триала на данный момент. Если триал все еще активен, то последний активный день на данный момент в формате даты, иначе `trial_ended`
        case when paid_accounts.account_id is null then false else True end as did_pay, -- Если у пользователя была оплата (paid_at is not null в billingPackages), то true, иначе false
        case when active_with_free_and_promised_payments_and_without_trials.account_id is null then false else True end as is_active    -- Если у пользователя на данный момент есть активная (платная, бесплатная или в обещанном платеже) подписка, то true, иначе false
from accounts_info
left join integrations 
            on accounts_info.account_Id = integrations.account_Id
left join active_integrations
            on accounts_info.account_Id = active_integrations.account_Id
left join not_deleted_and_errorless_integrations
            on accounts_info.account_id = not_deleted_and_errorless_integrations.account_id
left join all_channels
            on accounts_info.account_Id = all_channels.account_Id
left join count_active_channels 
        on accounts_info.account_id = count_active_channels.account_id
left join count_channels 
        on accounts_info.account_id = count_channels.account_id
left join int_accounts_minimum_trial_start_dates_situation
        on accounts_info.account_id = int_accounts_minimum_trial_start_dates_situation.account_id
left join paid_accounts 
        on accounts_info.account_id = paid_accounts.account_id
left join active_with_free_and_promised_payments_and_without_trials
        on accounts_info.account_id = active_with_free_and_promised_payments_and_without_trials.account_id
where account_segment_type not in ('дочка постоплатников','демо-партнёр','работник') 
    and accounts_info.account_Id not in (38783219,50838383)
    and is_employee is false