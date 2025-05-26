SELECT account_id                                                               -- аккаунт партнера
     , date_sub(occured_date, interval 1 MONTH)             AS paid_month       -- месяц. учитывается за тот период, во время которого был пополнен баланс, т.е. месяц назад date_sub(occured_date,interval 1 month)
     , billing_affiliate.currency                                               -- валюта ЛК партнера на момент прихода бонусов
     , abs(sum(sum * coalesce(exchange_rates.cor_rate, 1))) AS sum_in_rubles    -- сумма в рублях. курс берется на день выплаты
     , abs(sum(sum))                                        AS original_sum     -- сумма в валюте ЛК партнера на момент прихода бонусов

FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` billing_affiliate
LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates` exchange_rates ON exchange_rates.currency=billing_affiliate.currency 
                                                        AND billing_affiliate.occured_date=exchange_rates.data AND nominal='RUR'
WHERE object ='rewardWaba' -- 10%, которые отправляют партнерам при пополнении баланса вабы. Раз в месяц отрабатывает кронджоба, которая добавляет запись. Кронджоба отрабатывает в месяц следующий после фактического начисления. Например, у реферального папы есть дочка, которая купила ряд подписок в марте. За эти подписки реф папе придут бонусы в начале следующего месяца, то есть в апреле.
GROUP BY 1, 2, 3
    -- Вознаграждение, которое получают оф. и тех. партнеры за пополнение баланса Waba