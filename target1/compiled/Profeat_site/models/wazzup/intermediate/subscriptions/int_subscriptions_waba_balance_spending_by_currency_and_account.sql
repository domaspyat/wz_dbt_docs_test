SELECT       -- Таблица списаний с баланса WABA с указанными валютами и номером аккаунта
       account_id                                                               -- ID аккаунта
     , CAST(waba_channels.date_at AS DATE)                     AS spend_date    -- Дата списания
     , waba_channels.currency                                                   -- Валюта: RUR, USD, EUR, KZT
     , state
     , abs(sum(amount * coalesce(exchange_rates.cor_rate, 1))) AS sum_in_rubles -- Сумма в рублях
     , abs(sum(amount))                                        AS original_sum  -- Сумма в оригинальной валюте
FROM `dwh-wazzup`.`dbt_nbespalov`.`mart_channels_waba_spending` waba_channels
LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates` exchange_rates ON exchange_rates.currency=waba_channels.currency AND
                                                        CAST (waba_channels.date_at AS DATE)=exchange_rates.data AND nominal='RUR'
WHERE amount!=0 AND state in('paid','holded')
GROUP BY 1, 2, 3, 4