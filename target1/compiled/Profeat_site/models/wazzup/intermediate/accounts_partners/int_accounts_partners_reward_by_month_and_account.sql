SELECT account_id,                  -- аккаунт, которому пришло вознаграждение
    date_sub(date_trunc(occured_date,month),interval 1 month) as paid_month,    -- месяц. учитывается за тот период, во время которого был пополнен баланс, т.е. месяц назад date_sub(occured_date,interval 1 month)
    billing_affiliate.currency,     -- валюта ЛК партнера на момент прихода бонусов
    abs(sum(sum*coalesce(exchange_rates.cor_rate,1))) as sum_in_rubles,         -- сумма в рублях. курс берется на момент прихода бонусов
    abs(sum(sum)) as original_sum                                               -- сумма в валюте ЛК партнера на момент прихода бонусов

FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` billing_affiliate
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates` exchange_rates 
on exchange_rates.currency=billing_affiliate.currency and 
billing_affiliate.occured_date=exchange_rates.data and nominal='RUR'
where object='reward'
group by 1,2,3
    -- Кэшбэк за оплату подписок за месяц