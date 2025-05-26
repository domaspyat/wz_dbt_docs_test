with all_data as (
select cmuserid,
        event,
        count(*) events_count
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test`
where event in (
  'editor.link.click',
  'editor.tariff-change.click',
  'editor.banner-off.click',
  'editor.menu.click',
  'my-sites.created-page.click',
  'my-sites.analytics-disabled.click',
  'my-sites.analytics.click',
  'my-sites.statistics.click',
  'my-sites.copy-link.click',
  'my-sites.features.click',
  'my-sites.profile.click',
  'my-sites.create-new-pages-disabled.click',
  'my-sites.create-new-pages.click',
  'my-sites.notifications.click'
)
group by cmuserid,event),
conversions_base as (
  select cmuserid,
        count(case when event in ('editor.menu.click',  'my-sites.analytics.click',
                                                  'my-sites.statistics.click',
                                                  'my-sites.copy-link.click',
                                                  'my-sites.features.click',
                                                  'my-sites.profile.click',
                                                  'my-sites.notifications.click'
                            ) then cmuserid  end)  as events_to_menu_count,
         count(case when event in (
                            'my-sites.created-page.click',
                            'my-sites.create-new-pages.click'
                            ) then cmuserid  end) as events_to_editor_count
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test`
where event in (
  'editor.link.click',
  'editor.tariff-change.click',
  'editor.banner-off.click',
  'editor.menu.click',
  'my-sites.created-page.click',
  'my-sites.analytics-disabled.click',
  'my-sites.analytics.click',
  'my-sites.statistics.click',
  'my-sites.copy-link.click',
  'my-sites.features.click',
  'my-sites.profile.click',
  'my-sites.create-new-pages-disabled.click',
  'my-sites.create-new-pages.click',
  'my-sites.notifications.click'
)
group by cmuserid
)

,data_used_as_filters as(
    select * from `dwh-wazzup`.`dbt_swazzup`.`int_cmuserid_all_info_that_used_as_filters`
)
select data_used_as_filters.*,
        event,
        events_count,
        events_to_editor_count,
        events_to_menu_count
from  data_used_as_filters
join all_data on data_used_as_filters.cmuserid = all_data.cmuserid
join conversions_base on data_used_as_filters.cmuserid = conversions_base.cmuserid