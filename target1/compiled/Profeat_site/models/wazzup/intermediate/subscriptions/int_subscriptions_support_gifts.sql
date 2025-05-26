with billing_affiliates as (
     select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate`
 ),

 exchange_rates as (
     select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates`
 ),

 accounts as (
     select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
 )
            -- Таблица начислений и списаний от саппорта
 select date_trunc(occured_date,month) as occured_month,                                            -- Месяц события
        billing_affiliates.guid as subscription_id,                                                 -- guid из billingPackages или guid из таблицы payments если object - refund, bonus, transfer 
        billing_affiliates.account_id,                                                              -- ID аккаунта
        sum(coalesce(billing_affiliates.sum*cor_rate, billing_affiliates.sum)) as sum_in_rubles,    -- Сумма в рублях
        object                                                                                      -- Тип транзакции. Берем только начисления и списания саппортом
        from billing_affiliates
        inner join accounts on billing_affiliates.account_id = accounts.account_id
        left join exchange_rates on billing_affiliates.occured_date = exchange_rates._ibk
                                                            and billing_affiliates.currency = exchange_rates.currency
                                                            and nominal = 'RUR'
        where object in ('supportBonus','takeAway') and accounts.type != 'employee'
  group by 1,2,3,5