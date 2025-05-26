
    
    

with dbt_test__target as (

  select guid as unique_field
  from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_old_and_new_data_union`
  where guid is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


