SELECT
    account_id,
    PARSE_DATE('%d.%m.%Y', month)                          AS paid_date,
    currency,
    COALESCE(CAST(REPLACE(waba_sum, ' ', '') AS int), 0)   AS waba_sum_in_rubles
FROM `dwh-wazzup`.`google_sheets`.`postpay_paying`