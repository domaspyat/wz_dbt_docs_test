-- Таблица изменений партнеров/реф.пап с даты регистрации

with int_accounts_registration_data__first_parent_and_refparent as (            -- Тянем данные из таблицы дат регистраций, связями с партнером/реф.папой и датой окончания первых связей  
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_registration_data__first_parent_and_refparent`
),

partner_data_change as (            -- Тянем данные из таблицы с изменениями по партнерам аккаунтов
    select  
        account_id,             -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
        start_occured_at,       -- Дата и время изменения партнера
        end_occured_at,         -- Дата и время, до которого был текущий партнер
        partner_id,             -- Идентификатор аккаунта нового партнера
        refparent_id            -- Идентификатор аккаунта реферального папы
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts_events__partner_change` 
),

accounts__partner_change_and_register_history as (          -- Таблица изменений партнеров/реф.пап
    select  account_id,                                                 -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
        start_occured_at,                                               -- Дата и время начала действия первой связи с партнером и реф.папой
        cast(start_occured_at as date) as start_date,                   -- Дата начала действия первой связи с партнером и реф.папой
        end_occured_at,                                                 -- Дата и время конца действия первой связи с партнером и реф.папой
        cast(end_occured_at as date) as end_date,                       -- Дата конца действия первой связи с партнером и реф.папой
        partner_id,                                                     -- Идентификатор аккаунта первого партнера
        refparent_id                                                    -- Идентификатор аккаунта первого реферального папы
    from int_accounts_registration_data__first_parent_and_refparent 

    UNION ALL 

    select 
        account_id,                                                     -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
        start_occured_at,                                               -- Дата и время начала действия связи с партнером и реф.папой
        cast(start_occured_at as date) as start_date,                   -- Дата начала действия связи с партнером и реф.папой
        end_occured_at,                                                 -- Дата и время конца действия связи с партнером и реф.папой
        cast(end_occured_at as date) as end_date,                       -- Дата конца действия связи с партнером и реф.папой
        partner_id,                                                     -- Идентификатор аккаунта партнера
        refparent_id                                                    -- Идентификатор аккаунта реферального папы
    from partner_data_change 
)

select * from accounts__partner_change_and_register_history