select bills.account_id, 
paid_date,
sum(sum_in_rubles) as sum_in_rubles_invalid_bills
from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_bills` bills
inner join
    `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts on accounts.account_id = bills.account_id
where
    bills.paid_date <= date_add(accounts.register_date, interval 1 month)
    and accounts.type = 'standart' and status = 'paidInvalid'
group by 1,2