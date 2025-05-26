with active_users  as (
Select *
from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid__count_distinct_visitkas_visitors_for_the_last_30_days`
where DistinctVisits >= 3
)
select *
from active_users
unpivot (
  type for type_name in (client_type,client_type_paid,client_type_post)
)