with active_users_monthly as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid__active_users_monthly`
), 
phones as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_phone__last_value`
),
template_links as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_localuserid_template_link_groupped`
),
defining_drop_date as (
                select distinct 
                      active_users_monthly.cmuserid,
                      phone,
                      first_value(date) over (partition by active_users_monthly.cmuserid order by date desc) as drop_date,
                      template_link
                from active_users_monthly
                join  phones on active_users_monthly.cmuserId = phones.cmuserid
                join template_links on active_users_monthly.cmuserId = template_links.cmuserid
                          )
select *
from defining_drop_date