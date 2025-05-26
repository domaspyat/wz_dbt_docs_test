
with profile_info as (
    select account_Id,                  -- ID аккаунта
        accounts.register_date,         -- Дата регистрации
        users.phone,                    -- телефон пользователя, указанный при регистрации
        users.name,                     -- имя пользователя, указанное при регистрации
        accounts.regEmail as email,     -- почта пользователя, указанная при регистрации
        accounts.currency,              -- Валюта на текущий момент
        accounts.account_language,      -- язык ЛК пользователя, указанный на текущий момент
        city,                           -- город пользователя, как мы его определелили
        region,                         -- область пользователя, как мы его определи
        accounts.country,               -- страна пользователя, как мы её определили в формате alpha-2
        discount as partner_discount,   -- партнерская скидка (в случае, если этот аккаунт принадлежит партнеру) или null в случае непартнерского аккаунта
        region_type,                    -- общее определение регионов - CIS - СНГ, non-CIS - все страны вне СНГ
        demo_account,                   -- account_id демо-аккаунта партнера (в случае, если этот аккаунт принадлежит партнеру (type partner,tech-partner,tech-partner-postpay)) или null в случае непартнерского аккаунта
        accounts.type,                  -- Тип аккаунта
        utm_source,
        utm_campaign,
        utm_medium,
        utm_term,
        utm_content, 
        is_activated_by_email
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts
    left join `dwh-wazzup`.`dbt_nbespalov`.`stg_users` users on users.email=accounts.regEmail

), paying_segments as (
    select distinct account_id,
            row_number() over(partition by account_id order by start_date desc) rn ,
            partner_id,
            refparent_id,
            segment as segment_type
     from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_who_paid_segments_with_partner_type_and_account_type`), 

country as (select * from `dwh-wazzup`.`analytics_tech`.`country`),

attribution_data as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_defining_clients_types` 
),

active_integrations as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_active_groupped_by_accounts_type`

),

last_integrations as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_last_created_integration_by_account_id`

),

account_leaving_situation as (
    select          account_id, 
                    clients_type_for_communications,
                    data_otvala,
                    row_number() over (partition by account_Id order by subscription_start desc) arn
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_defining_clients_types`
    ),

paid_subscription__type_with_type_and_tarif as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_paid_subscription_with_type_and_tariff`
),

    
profile_info_with_accounts_type as (
    select profile_info.*, 
            accounts_demo.register_date as partner_register_date,   -- дата регистрации партнера (если у account_id type: partner,tech-partner,tech-partner-postpay)
            russianName	as russian_country_name,                    -- название страны из country на русском языке
            country.region as region_international,                 -- регион пользователя (СНГ, МЕНА, ЛАТАН ) согласно нашему внутреннему разделению
            paying_segments.partner_id,                             -- ID партнера
            paying_segments.refparent_id,                           -- ID реф. партнера
            active_integrations.active_integration_name,            -- интеграция, активная в данный момент в ЛК пользователя
            coalesce((
                case when active_integrations.active_integration_name='does_not_have_an_active_integration' then null else 
                active_integrations.active_integration_name end), last_integrations.last_integration_name) as last_integration_name,    -- интеграция, активная в данный момент в ЛК пользователя, либо последняя созданная интеграция, если нет активной
            case 
                 when profile_info.type = 'partner-demo' then 'демо-партнёр'
                 when profile_info.type = 'partner' then 'оф. партнёр'
                 when profile_info.type = 'employee' then 'работник'
                 when profile_info.type = 'tech-partner' then 'обычный техпартнер'
                 when profile_info.type = 'child-postpay' then 'дочка постоплатников'
                 when profile_info.type = 'tech-partner-postpay' then 'техпартнер-постоплатник'
                 when segment_type ='standart_without_partner' then 'обычный (юзер без партнера)'
                 when segment_type = 'tech_partner_child__tech_partner_paid' then 'Дочка тех партнёра, которая платит через тех партнёра'
                 when segment_type = 'tech_partner_child__child_paid' then 'Дочка тех партнёра, которая платит сама'
                 when segment_type = 'partner' then 'оф. партнёр'
                 when segment_type = 'employee' then 'работник'
                 when segment_type = 'partner-demo' then 'демо-партнёр'
                 when segment_type = 'of_partner_child__of_partner_paid' then 'Дочка, которая платит через партнёра'
                 when segment_type = 'of_partner_child_child_paid' then 'Самодостаточная дочка оф. партнера , которая платит сама'
                 when segment_type = 'tech-partner' then 'обычный техпартнер'
                 when segment_type = 'tech-partner-postpay' then 'техпартнер-постоплатник'
            else 'unknown' end as account_segment_type,         -- сегмент пользователя
            coalesce(clients_type_for_communications,'never_bought_a_subscription') as account_leaving_situation_type,  -- Тип клиента: still_active, bought_new_subscription_within_leaving_period, came_back_after_leaving_period, did_not_come_back, may_become_active
            accounts_partner_type.type as partner_account_type  -- type партнера, если он есть (null иначе)
            from profile_info 
            left join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts_demo
                    on accounts_demo.account_id=profile_info.demo_account
            left join paying_segments 
                    on profile_info.account_id = paying_segments.account_id
                    and rn = 1
            left join country 
                    on country.iso2=profile_info.country
            left join account_leaving_situation
                    on profile_info.account_id = account_leaving_situation.account_Id
                    and arn = 1
            left join active_integrations
                    on active_integrations.account_id=profile_info.account_id
            left join last_integrations
                    on last_integrations.account_id=profile_info.account_id   
            left join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts_partner_type 
            on accounts_partner_type.account_id=paying_segments.partner_id                   
    ),
test_accounts as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_test_accounts`
),

first_subscription_date_with_1_more_day_duration as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_first_subscription_date_with_more_than_1_day_duration`
),

profile_info_with_country as (
select profile_info_with_accounts_type.*,
        paid_subscription__type_with_type_and_tarif.type_and_tariff as paid_subscription_type_and_tarif,
        (case 
        when currency is not null then currency
        when currency is null and profile_info_with_accounts_type.country='RU' then 'RUR'
        when currency is null and profile_info_with_accounts_type.country='kz' then 'KZT'
        when currency is null and profile_info_with_accounts_type.country in ('au', 'be', 'bg', 'hu', 'de', 'gr', 'dk', 'ie', 'es', 'it',
        'cy', 'lv', 'lt', 'lu', 'mt', 'nl', 'pl', 'pt', 'sk', 'si',
        'fi', 'fr', 'hr', 'cz', 'se', 'ee', 'no', 'gb', 'is', 'li',
        'ch', 'ad', 'mc', 'sm', 'gi') then 'EUR'
        else 'USD'
        end
        ) as account_currency_by_country, --https://wazzup24.atlassian.net/wiki/spaces/WAZ/pages/2563250
        registration_source_agg_current, --агрегированный источник регистрации на текущий момент (может измениться тип аккаунта)
        registration_source_current,  --агрегированный источник регистрации на текущий момент (может измениться тип аккаунта)
        registration_source_agg, --агрегированный источник регистрации на момент регистрации (может измениться тип аккаунта)
        registration_source,    -- источник регистрации на момент регистрации (может измениться тип аккаунта)
        (case when profile_info_with_accounts_type.type = 'employee' or test_accounts.account_Id is not null then True else False end) as is_employee,
        first_subscription_date_with_1_more_day_duration.min_paid_date as first_paid_subscription_date_with_1_more_day_duration

from profile_info_with_accounts_type
left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_attribution_data` attribution_data on attribution_data.account_id=profile_info_with_accounts_type.account_id
left join test_accounts on profile_info_with_accounts_type.account_Id = test_accounts.account_Id
left join first_subscription_date_with_1_more_day_duration on first_subscription_date_with_1_more_day_duration.account_id=profile_info_with_accounts_type.account_id
left join paid_subscription__type_with_type_and_tarif on paid_subscription__type_with_type_and_tarif.account_id=profile_info_with_accounts_type.account_id

)
    -- Таблица, в котором собрана информация об аккаунте
select *except(utm_campaign),
        case when utm_campaign like '%%D0%%' then REGEXP_EXTRACT(utm_campaign, r'(\d+)$') else utm_campaign end as utm_campaign
from profile_info_with_country