Select distinct
        cast(dt.date as date) date,
        te.cmuserid,
        te.localuserid as visitkas_users
from `dwh-wazzup`.`analytics_tech`.`days` dt
left join `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_visitkas_visitors_with_visit_time` te on cast(dt.date as date) = te.date
where  te.cmuserid is not null