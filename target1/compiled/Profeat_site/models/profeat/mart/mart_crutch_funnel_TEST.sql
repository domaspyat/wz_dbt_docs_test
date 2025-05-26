with int_funnel_key_events__finding_all_users_stages as  (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_funnel_key_events__finding_all_users_stages`
    ),

int_funnel_key_events__counting_users_on_each_stage as (
    select
    registration_date,
    utm_source,
    utm_campaign,
    utm_medium,
    initrefferer,
    utm_traffic,
    abtest_name,
    abtest_group,
    abgroup_count_filter,
    case
        when devicetypes = 'tablet' or os = 'Android'
        then 'mobile'
        when devicetypes is not null
        then devicetypes
        when os is null or os = 'Other'
        then 'other'
        when
            os = 'Linux'
            or os = 'Windows'
            or os = 'Ubuntu'
            or os = 'Mac OS'
            or os = 'Mac OS X'
        then 'desktop'
    end as devicetypes,
    template_link,
    business_spheres_filter,
    eventgroupname_description,
    business_spheres_filter_description,
    count(distinct first_paid) as first_paid,
    count(distinct repeat_paid) as repeat_paid,
    count(distinct succreg) as SuccReg,
    count(distinct templateusage) as TemplateUsage,
    count(distinct edits) as Edits,
    count(distinct activation) as Activation,
    count(distinct copies) as Copies,
    count(distinct posted) as Posted,
    count(distinct paid) as Paid,
    
    from int_funnel_key_events__finding_all_users_stages
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12,13,14
    )
,int_funnel_key_event__unpivot_events as (
    select * from int_funnel_key_events__counting_users_on_each_stage 
    unpivot (
            users for event in (SuccReg,TemplateUsage,Edits,Activation,Copies,Posted,paid,repeat_paid,first_paid)
            )
    )

SELECT `profeat_site_funnel`.`Business_spheres_filter` AS `Business_spheres_filter`,
  `profeat_site_funnel`.`Business_spheres_filter_description` AS `Business_spheres_filter_description`,
  `profeat_site_funnel`.`EventGroupName_Description` AS `EventGroupName_Description`,
  `profeat_site_funnel`.`deviceTypes` AS `deviceTypes`,
  `profeat_site_funnel`.`event` AS `event`,
  `profeat_site_funnel`.`initRefferer` AS `initRefferer`,
  `profeat_site_funnel`.`registration_date` AS `regdate`,
  `profeat_site_funnel`.`template_link` AS `template`,
  `profeat_site_funnel`.`users` AS `users`,
  `profeat_site_funnel`.`utm_campaign` AS `utm_campaign`,
  `profeat_site_funnel`.`utm_medium` AS `utm_medium`,
  `profeat_site_funnel`.`utm_source` AS `utm_source`,
  `profeat_site_funnel`.`utm_traffic` AS `utm_traffic`
FROM int_funnel_key_event__unpivot_events `profeat_site_funnel`

Union all 
SELECT `profeat_site_funnel`.`Business_spheres_filter` AS `Business_spheres_filter`,
  `profeat_site_funnel`.`Business_spheres_filter_description` AS `Business_spheres_filter_description`,
  `profeat_site_funnel`.`EventGroupName_Description` AS `EventGroupName_Description`,
  `profeat_site_funnel`.`deviceTypes` AS `deviceTypes`,
  
  case when `profeat_site_funnel`.`event` = 'Posted' then 'posted_fix' 
        when `profeat_site_funnel`.`event` = 'TemplateUsage' then 'template_fix' 
        when `profeat_site_funnel`.`event` = 'paid' then 'paid_fix' 
        when `profeat_site_funnel`.`event` = 'Activation' then 'activation_fix' 
        when `profeat_site_funnel`.`event` = 'Edits' then 'edits_fix' 
       when `profeat_site_funnel`.`event` = 'SuccReg' then 'succreg_fix' 
      when `profeat_site_funnel`.`event` = 'Copies' then 'copies_fix' 
  end AS `event`,
  `profeat_site_funnel`.`initRefferer` AS `initRefferer`,
  `profeat_site_funnel`.`registration_date` AS `regdate`,
  `profeat_site_funnel`.`template_link` AS `template`,
  `profeat_site_funnel`.`users` AS `users`,
  `profeat_site_funnel`.`utm_campaign` AS `utm_campaign`,
  `profeat_site_funnel`.`utm_medium` AS `utm_medium`,
  `profeat_site_funnel`.`utm_source` AS `utm_source`,
  `profeat_site_funnel`.`utm_traffic` AS `utm_traffic`
FROM int_funnel_key_event__unpivot_events `profeat_site_funnel`