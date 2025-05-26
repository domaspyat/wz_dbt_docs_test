with visitkas_visitors_with_visit_time as 
    (select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid__visitkas_visitors_with_visit_time`
    )
select cmuserid, 
        date_trunc(cast(datetime as date),month) date,
        count(distinct localUserId) as visitkas_users 
from visitkas_visitors_with_visit_time
where cmuserid is not null
group by 1,2
having count(distinct localUserId) >= 3