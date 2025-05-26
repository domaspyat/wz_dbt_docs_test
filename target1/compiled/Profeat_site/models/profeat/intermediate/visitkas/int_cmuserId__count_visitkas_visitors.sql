with visitkas_visitors_with_visit_time as 
    (select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid__visitkas_visitors_with_visit_time`
    ),

    visitkas_visitors_with_visit_time_deduplicated as 
    (select cmuserid, 
            template_link,
            first_time_value_over_localuserid,
            dense_rank() over (partition by cmuserid,template_link order by first_time_value_over_localuserid,localuserid) rank,
            localuserid,
            count(distinct localUserId) over (partition by template_link,cmuserid) as visitkas_users 
            from visitkas_visitors_with_visit_time
    )
select distinct cmuserid,template_link,first_time_value_over_localuserid as activation_datetime
from visitkas_visitors_with_visit_time_deduplicated
where rank = 3
and cmuserid is not null