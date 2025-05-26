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
    datetime
    from template_link_groupped 
    left join visitka_enter
    on template_link_groupped.template_link = visitka_enter.url
    and visitka_enter.localuserid != template_link_groupped.localuserid
    and (visitka_enter.cmuserid != template_link_groupped.cmuserid or visitka_enter.cmuserid is null)