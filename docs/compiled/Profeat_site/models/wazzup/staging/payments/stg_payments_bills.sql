select          -- Таблица счетов
    accountId as account_id,                -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    coalesce((case when paymentDate='1970-01-01' then cast(null as date) else paymentDate end),cast(updatedAt as date)) paid_date,  -- Дата платежа (от банка). Если она отсутствует, берется дата обновления счета, формат 2022-11-29
    (case when id=10807 then  cast('2022-05-09' as timestamp)       -- *персональные изменения для конкретных счетов*
    when id=10118 then  cast('2022-05-09' as timestamp)
    else paidInWazzupAt
    end
    ) as paid_in_wazzup_at,         
    completedAt as completed_at,                            -- Дата и время оплаты счета
    'RUR' as currency,                                      -- Валюта (Везде рубли)
    (case when id=88070 then 8100               -- *эти изменения делались в рамках правок найденных расхождений в январе https://wazzup.planfix.ru/task/1105536* 
    when id=88060 then 9000
    when id=88099 then 8100
    when id=87628 then 10530
    else sum                                              
    end)
    as sum_in_rubles,                                           -- Сумма счета. Это поле sum из wazzup.biils. Используется в дальнейших расчетах, поэтому разделено на sum_in_rubles/original_sum
    (case when id=88070 then 8100                -- *эти изменения делались в рамках правок найденных расхождений в январе https://wazzup.planfix.ru/task/1105536*
    when id=88060 then 9000
    when id=88099 then 8100
    when id=87628 then 10530
    else sum 
    end) as original_sum,                                       -- Сумма счета. Это поле sum из wazzup.biils. Используется в дальнейших расчетах, поэтому разделено на sum_in_rubles/original_sum
    cast(id as STRING) as guid,                                 -- Идентификатор счета.Генерируется Postgress при создании записи в формате string
    packageId as subscription_id,                               -- guid подписки, соответствует guid из stg_billingPackages
    updatedAt as updated_at,                                    -- Дата обновления счета
    status,                                                     -- Состояние счета
    details,                                                    -- Детали платежа
    id                                                          -- Идентификатор счета.Генерируется Postgress при создании записи в формате int
from `dwh-wazzup`.`wazzup`.`bills`