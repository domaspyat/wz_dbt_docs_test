
    
    

with dbt_test__target as (

  select account_id as unique_field
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_first_payment_billing`
  where account_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


