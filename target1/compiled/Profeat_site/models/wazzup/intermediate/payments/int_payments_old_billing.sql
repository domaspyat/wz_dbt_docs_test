select  -- Продвинутая таблица биллинга. Последняя запись в 2022
    account_id,     -- ID аккаунта
    paid_date,      -- Дата платежа
    currency,       -- Валюта
    sum_in_rubles,  -- Сумма платежа в рублях
    paid_at,        -- Дата и время платежа
    (case when currency='RUR' then sum*40
    else sum
    end) as original_sum, -- Сумма платежа
    guid,           -- guid платежа
    (case when json_value(details,'$.provider') like '%account%' then split(json_value(details,'$.provider'),'_')[OFFSET(1)]
        end
    ) as payment_method,  -- ID аккаунта партнера, если он платил
    provider,       -- Провайдер оплаты
    object,         -- Предмет платежа
    method,         -- Метод платежа
    start_at,       -- Дата начала подписки
    end_at          -- Дата окончания подписки
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_old_billing`