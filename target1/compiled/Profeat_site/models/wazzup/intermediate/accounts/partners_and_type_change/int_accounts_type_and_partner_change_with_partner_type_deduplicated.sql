-- Таблица изменений связей партнеров/реф.пап, типов аккаунта, типов аккаунта партнера начиная с создания аккаунта (добавлены исключения)
 -- Берем только одно изменение параметров аккаунта в один день. Берем то, которое заканчивается позже.
-- Материализуем эту модель как таблицу


WITH accounts_type_and_partner_change_with_partner_type AS (            -- Таблица изменений связей партнеров/реф.пап, типов аккаунта, типов аккаунта партнера начиная с создания аккаунта (добавлены исключения)
    SELECT 
        account_id,             -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
        partner_id,             -- Идентификатор аккаунта партнера
        refparent_id,           -- Идентификатор аккаунта реферального папы
        account_type,           -- Тип аккаунта
        partner_type,           -- Тип партнера
        end_occured_at,         -- Дата и время окончания действия данного типа аккаунта
        start_occured_at,       -- Дата и время начала действия изменения
        end_date,               -- Дата окончания действия данного типа аккаунта
        (CASE 
            WHEN account_id=42256887 AND account_type='tech-partner' -- *добавила как костыль, пока не разберусь, почему там account_type - null* старый коммент
                THEN DATE('2023-12-28')            
            WHEN account_id=31258487 AND account_type='standart' AND partner_type='partner' AND start_date=DATE('2024-03-12') 
                THEN DATE('2024-02-20')
            ELSE start_date
        END
        ) AS start_date,       -- Дата начала действия изменения

    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type`     -- Тянем данные из таблицы изменений связей партнеров/реф.пап, типов аккаунта, типов аккаунта партнера начиная с создания аккаунта
),

accounts_type_and_partner_change_with_partner_type_to_deduplicate AS (          -- Пронумерованная "Таблица изменений связей партнеров/реф.пап, типов аккаунта, типов аккаунта партнера начиная с создания аккаунта (добавлены исключения)"
    SELECT 
        *,      -- Тянем все данные
        row_number() OVER (partition by account_id, start_date ORDER BY end_occured_at desc) AS rn  -- Нумеруем по полям account_id и start_date. Чем позже заканчивается, тем меньше номер.
                                                                                                    -- Нумеруем, чтобы отсеять изменения сделанные в один день
    FROM accounts_type_and_partner_change_with_partner_type

    WHERE start_date != end_date            -- Не берем изменения длинной меньше суток, то есть те, которые начались и закончились в один день.
),    
accounts_type_and_partner_change_with_partner_type_deduplicated AS (
    SELECT 
        account_id,                     -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
        (CASE 
            WHEN partner_id=0 
                THEN null
            ELSE partner_id             
        END) AS partner_id,             -- Идентификатор аккаунта партнера (null, если значение "0")
        (CASE 
            WHEN refparent_id=0 
                THEN null
            ELSE refparent_id 
        END) AS refparent_id,           -- Идентификатор аккаунта реферального папы (null, если значение "0")
        account_type,                   -- Тип аккаунта
        partner_type,                   -- Тип партнера
        end_occured_at,                 -- Дата и время окончания действия данного типа аккаунта
        start_occured_at,               -- Дата и время начала действия изменения
        end_date,                       -- Дата окончания действия данного типа аккаунта
        start_date                      -- Дата начала действия изменения
        FROM accounts_type_and_partner_change_with_partner_type_to_deduplicate
    WHERE rn=1                  -- Берем только одно изменение параметров аккаунта в один день. Берем то, которое заканчивается позже.
)
SELECT *
FROM accounts_type_and_partner_change_with_partner_type_deduplicated