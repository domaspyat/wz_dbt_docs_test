-- Таблица всех изменений типов и связей с партнерами

with partner_data as (          -- Тянем все данные из таблицы изменений партнеров/реф.пап с даты регистрации без повторов в рамках одного start_date для каждого из аккаунтов
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__partner_and_refparent_change_deduplicated`
),

type_change as (                -- Тянем все данные из таблицы историй изменений типов аккаунтов без повторов в рамках одного start_date для каждого из аккаунтов
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_acccounts__type_change_deduplicated`
),

type_and_partner_change as (            -- Объединяем таблицы изменений связей с партнерами/реф.папами и изменений типов аккаунтов
    select 
        coalesce(partner_data.account_id,type_change.account_id) as account_id ,                        -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts 
        coalesce(partner_data.start_occured_at, type_change.start_occured_at) as start_occured_at,      -- Дата и время начала действия изменения

        (case 
            when type_change.end_occured_at < coalesce(type_change.end_occured_at,partner_data.end_occured_at) then type_change.end_occured_at
            when partner_data.end_occured_at < coalesce(type_change.end_occured_at,partner_data.end_occured_at) then partner_data.end_occured_at
            else coalesce(type_change.end_occured_at,partner_data.end_occured_at)
        end) as end_occured_at,         -- Дата и время конца действия изменения 
                                        -- Берем наименьшую дату из доступных (конец связей партнера/реф.папы или конец действия типа аккаунта), то есть более раннюю дату. Если доступна одна (вторая null), то берем доступную 

        partner_data.partner_id,                -- Идентификатор аккаунта партнера
        partner_data.refparent_id,              -- Идентификатор аккаунта реферального папы
        type_change.type as account_type        -- Тип аккаунта
    from  partner_data 
        full outer join type_change         -- Объединяем таблицы так, чтобы были соответствия всем строкам из двух исходных таблиц. 
            on partner_data.account_id=type_change.account_id
                and partner_data.start_date>=type_change.start_date
                and partner_data.start_date<=type_change.end_date
)


select          
    *,       
    cast(start_occured_at as date) as start_date,           -- Дата начала действия изменения
    cast(end_occured_at as date) as end_date                -- Дата конца действия изменения 
 from type_and_partner_change