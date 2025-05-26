
    
    

with dbt_test__target as (

  select cmuserid as unique_field
  from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_business_spheres_only_other`
  where cmuserid is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


