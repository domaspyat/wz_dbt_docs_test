SELECT ba.account_id
     , ba.occured_date
     , ba.occured_at
     , ba.currency
     , sum                      AS original_sum
     , coalesce(sum * rur, sum) AS sum_in_rubles
FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
    LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted
            ON exchange_rates_unpivoted._ibk = ba.occured_date
    AND exchange_rates_unpivoted.currency = ba.currency
WHERE object = 'withdrawal'