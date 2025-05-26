-- Таблица изменений партнеров/реф.пап с даты регистрации без повторов в рамках одного start_date для каждого из аккаунтов
-- Таблица нужна для того чтобы исключить изменения, которые продержались менее 1 дня, то есть были сделаны в один start_date. Берем последнее.

with int_accounts__partner_change_and_register_data as (            -- Тянем все данные из таблицы изменений партнеров/реф.пап с даты регистрации
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__partner_change_and_register_history`
),

partner_change_and_register_data_to_deduplicate as (            -- Таблица с нумерацией строк с повторяющимися start_date для аккаунтов
    select 
        * ,                                                                                             -- Берем все данные
        row_number() over (partition by account_id, start_date  order by end_occured_at desc) as rn     -- Нумеруем все строки с одинаковыми полями account_id, start_date сортируя по полю end_occured_at
                                                                                    -- Чем ниже номер, тем позднее произошло изменение
    from int_accounts__partner_change_and_register_data
),

partner_change_and_register_data_deduplicated as (          -- Таблица изменений партнеров/реф.пап с даты регистрации без повторов в рамках одного start_date для каждого из аккаунтов
    select * from partner_change_and_register_data_to_deduplicate       -- Берем все данные

    where rn=1                                                          -- Только последнее изменение в рамках одного start_date
)

select * from partner_change_and_register_data_deduplicated