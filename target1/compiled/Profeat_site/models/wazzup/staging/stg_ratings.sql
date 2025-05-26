select          -- Таблица с оценками и отзывами клиентов Wazzup
    guid,               -- Идентификатор отзыва. Генерируется Postgres при создании записи
    account_id,         -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    rating,             -- Рейтинг. От 1 до 10
    text,               -- Текст отзыва 
    created_at,         -- Дата и время, в которое была поставлена оценка
    _ibk,               -- Дата создания отзыва. Необходимо для партицирования данных в BigQuery
    row_number() over (partition by account_id, _ibk order by created_at desc) as rn        -- Порядковый номер. Чем выше значение, тем раньше зафиксирован отзыв. 

 from `dwh-wazzup`.`wazzup`.`ratings`