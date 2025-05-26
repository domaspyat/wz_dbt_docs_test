with template_link_groupped as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_localuserid_template_link_groupped`
    ),

visitka_enter as (
    select template_visits.* except(cmuserid),
    coalesce(template_visits.cmuserid,grp.cmuserid) cmuserid
    from  `dwh-wazzup`.`dbt_nbespalov`.`stg_template_visitors` template_visits
    left join template_link_groupped grp on template_visits.localuserid = grp.localuserid
                                            and template_visits.url = grp.template_link

    )

select template_link_groupped.cmuserid, 
    template_link,
    visitka_enter.localuserid,
    date,
    datetime,
    min(datetime) over (partition by template_link_groupped.cmuserId order by dateTime) activation_datetime
    from template_link_groupped 
    join visitka_enter
    on template_link_groupped.template_link = visitka_enter.url
where template_link_groupped.cmuserId is not null
order by template_link_groupped.cmuserId