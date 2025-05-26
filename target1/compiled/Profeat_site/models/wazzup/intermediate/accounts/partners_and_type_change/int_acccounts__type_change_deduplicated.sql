-- Таблица историй изменений типов аккаунтов без повторов в рамках одного start_date для каждого из аккаунтов
-- Таблица нужна для того чтобы исключить изменения, которые продержались менее 1 дня, то есть были сделаны в один start_date. Берем последнее.

with type_change_history as (           -- Тянем все данные из таблицы историй изменений типов аккаунтов
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history`
),

type_change_history_to_deduplicate as (         -- Таблица с пронумерованными строками
    select * ,
    row_number() over (partition by account_id,start_date  order by end_occured_at desc) as rn      -- Нумеруем по полям Id аккаунта и датой старта типа. 
                                                                                                    -- Чем ниже значение, тем позже произошло изменение.
    from type_change_history
),

type_change_history_deduplicated as (           -- Таблица историй изменений типов аккаунтов. Взяты только последние изменения типов в каждом дне.
    select * from type_change_history_to_deduplicate            -- Тянем все данные с пронумерованными строками
    where rn=1                                                  -- Берем только первое с конца значение, то есть последнее изменение типа у каждого аккаунта
    )

select * from  type_change_history_deduplicated