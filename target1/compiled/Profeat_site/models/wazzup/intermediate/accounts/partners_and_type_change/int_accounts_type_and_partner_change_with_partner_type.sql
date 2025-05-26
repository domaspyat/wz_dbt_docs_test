-- Таблица изменений связей партнеров/реф.пап, типов аккаунта, типов аккаунта партнера начиная с создания аккаунта

with type_and_account_type_merged as (          -- Тянем все данные из таблицы всех изменений типов и связей с партнерами
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_history`
),

type_change_history_deduplicated as (           -- Тянем все данные из таблицы историй изменений типов аккаунтов без повторов в рамках одного start_date для каждого из аккаунтов
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_acccounts__type_change_deduplicated`
),

accounts_type_and_partner_change_with_partner_type as (         -- Таблица изменений связей партнеров/реф.пап, типов аккаунта, типов аккаунта партнера
    select 
        type_and_account_type_merged.account_id,            -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts 
        type_and_account_type_merged.partner_id,            -- Идентификатор аккаунта партнера
        type_and_account_type_merged.refparent_id,          -- Идентификатор аккаунта реферального папы
        type_and_account_type_merged.account_type,          -- Тип аккаунта
        partner_type_change.type as partner_type,           -- Тип аккаунта партнера
        (case 
            when partner_type_change.end_date <= type_and_account_type_merged.end_date then partner_type_change.end_occured_at
            else coalesce(type_and_account_type_merged.end_occured_at, partner_type_change.end_occured_at)
        end) as end_occured_at,                             -- Дата и время окончания действия изменения
                                                                -- Если дата окончания действия изменения типа аккаунта партнера раньше или в тот же день, то берем дату и время окончания действия изменения типа аккаунта партнера.
                                                                -- Иначе берем дату и время окончания действия изменения самого аккаунта. (Или любое not null значение)
        (case 
            when partner_type_change.start_occured_at >= type_and_account_type_merged.start_occured_at then  partner_type_change.start_occured_at
            else coalesce(type_and_account_type_merged.start_occured_at,  partner_type_change.start_occured_at)
        end) as start_occured_at,                            -- Дата и время начала действия изменения
                                                                -- Если время начала действия изменения типа аккаунта партнера позже или в тот же момент, то берем дату и время начала действия изменения типа аккаунта партнера.
                                                                -- Иначе берем дату и время начала действия изменения самого аккаунта. (Или любое not null значение)

        (case 
            when partner_type_change.end_occured_at <= type_and_account_type_merged.end_occured_at then partner_type_change.end_date
            else coalesce(type_and_account_type_merged.end_date, partner_type_change.end_date)
        end) as end_date,                                   -- Дата окончания действия изменения
                                                                -- Если дата окончания действия изменения типа аккаунта партнера раньше или в тот же день, то берем дату окончания действия изменения типа аккаунта партнера.
                                                                -- Иначе берем дату окончания действия изменения самого аккаунта. (Или любое not null значение)

        (case 
            when partner_type_change.start_occured_at >= type_and_account_type_merged.start_occured_at then  partner_type_change.start_date
            else coalesce(type_and_account_type_merged.start_date,  partner_type_change.start_date)
        end)
        as start_date                                       -- Дата начала действия изменения
                                                                -- Если время начала действия изменения типа аккаунта партнера позже или в тот же момент, то берем дату начала действия изменения типа аккаунта партнера.
                                                                -- Иначе берем дату начала действия изменения самого аккаунта. (Или любое not null значение)

    from type_and_account_type_merged
    left join type_change_history_deduplicated partner_type_change
        on type_and_account_type_merged.partner_id = partner_type_change.account_id
            and type_and_account_type_merged.start_date <= partner_type_change.end_date
),


min_partner_type_to_deduplicate as (            -- Таблица историй изменений типов аккаунтов с нумерацией по датам изменений у каждого аккаунта
  select 
    *,                                                                                  -- Берем все данные из таблицы историй изменений типов аккаунтов без повторов в рамках одного start_date для каждого из аккаунтов
    row_number() over (partition by account_id order by start_date asc) as first_rn     -- Нумеруем все строки у каждого аккаунта. Чем раньше изменение, тем меньше номер. То есть "1" у первого изменения.
  from type_change_history_deduplicated 
),

min_partner_type as (           -- Таблица первых изменений типов аккаунтов
  select * from min_partner_type_to_deduplicate             -- Берем все данные из таблицы историй изменений типов аккаунтов с нумерацией по датам изменений у каждого аккаунта
  where first_rn=1                                          -- Берем только с первым номером, то есть только первое изменение (в момент создания аккаутна)
),

    
accounts_type_and_partner_change_with_partner_type_without as (         -- Таблица данных (тип, партнер, реф.папа) только первое состояние. Дата конца = дата конца первого состояния. 
    select 
        type_and_account_type_merged.account_id,                                -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts 
        type_and_account_type_merged.partner_id,                                -- Идентификатор аккаунта партнера
        type_and_account_type_merged.refparent_id,                              -- Идентификатор аккаунта реферального папы
        type_and_account_type_merged.account_type,                              -- Тип аккаунта
        cast(null as string) as partner_type,                                   -- Тип партнера, всегда null значение, просто добавляем поле
        partner_type_change.start_occured_at as end_occured_at,                 -- Дата и время окончания действия данного типа аккаунта (Дата и время первого изменения)
        type_and_account_type_merged.start_occured_at as start_occured_at,      -- Дата и время начала действия изменения
        partner_type_change.start_date as end_date,                             -- Дата окончания действия данного типа аккаунта(Дата первого изменения)
        type_and_account_type_merged.start_date as start_date                   -- Дата начала действия изменения

    from type_and_account_type_merged
        left join min_partner_type partner_type_change
            on type_and_account_type_merged.partner_id = partner_type_change.account_id         -- Добавляем к строкам аккаунтов информацию о первых изменениях аккаунта

    where type_and_account_type_merged.start_date < partner_type_change.start_date          -- Берем только строки, в которых аккаунт был создан раньше, чем произошло первое изменение
),

accounts_type_and_partner_change_with_partner_type_without_to_deduplicate as (          -- Пронумерованная таблица данных (тип, партнер, реф.папа) только первое состояние. Дата конца = дата конца первого состояния. 
    select 
        *, 
        row_number() over (partition by account_id order by end_date asc) rn            -- Нумерация по id аккаунта, если значений несколько, то чем раньше закончилось, тем меньше значение
    from accounts_type_and_partner_change_with_partner_type_without
),

accounts_type_and_partner_change_with_partner_type_without_deduplicated as (            -- Таблица данных (тип, партнер, реф.папа) только первое состояние. Дата конца = дата конца первого состояния. Избавлена от дубликатов. 
    select 
        account_id,                 -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts 
        partner_id,                 -- Идентификатор аккаунта партнера
        refparent_id,               -- Идентификатор аккаунта реферального папы
        account_type,               -- Тип аккаунта
        partner_type,               -- Тип партнера
        end_occured_at,             -- Дата и время окончания действия данного типа аккаунта
        start_occured_at,           -- Дата и время начала действия изменения
        end_date,                   -- Дата окончания действия данного типа аккаунта
        start_date                  -- Дата начала действия изменения
        
    from accounts_type_and_partner_change_with_partner_type_without_to_deduplicate

    where rn=1                      -- Берем только строки, где end_date заканчивается раньше, чтобы не было дублей
),
accounts_type_and_partner_change as (       -- Таблица изменений связей партнеров/реф.пап, типов аккаунта, типов аккаунта партнера начиная с создания аккаунта
  
select * from accounts_type_and_partner_change_with_partner_type
UNION ALL
select * from accounts_type_and_partner_change_with_partner_type_without_deduplicated
)

select * from accounts_type_and_partner_change