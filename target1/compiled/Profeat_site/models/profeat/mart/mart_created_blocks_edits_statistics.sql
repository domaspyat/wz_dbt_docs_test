with users_stages as (
    select 
        cmuserid,
        utm_source,
        utm_campaign,
        utm_medium,
        initRefferer,
        utm_traffic,
        abtest_name,
        abtest_group,
        abgroup_count_filter,
        devicetypes,
        template_link,
        business_spheres_filter,
        eventgroupname_description,
        business_spheres_filter_description,
        registration_date
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_all_info_that_used_as_filters`
    )
select blocks.cmuserid,
      users_stages.registration_date,
      case 
           when event = 'editor.main-edit.click' then 'hat_block_edited'
           when event = 'editor.text-edit.click' then 'text_block_edited'
           when event = 'editor.socials-edit.click' then 'socials_block_edited'
           when event = 'editor.messengers-edit.click' then 'messengers_block_edited'
           when event = 'editor.button-edit.click' then 'link_block_edited'
           when event = 'editor.photos-edit.click' then 'photos_block_edited'
           when event = 'editor.video-edit.click' then 'videos_block_edited'
           when event = 'editor.separator-edit.click' then 'separator_block_edited'
           when event = 'editor.price-edit.click' then 'price_block_edited'
           when event = 'editor.map-edit.click' then 'map_block_edited'
           when event = 'editor.products-edit.click' then 'products_block_edited'
           when event = 'editor.faq-edit.click' then 'faq_block_edited'
           when event = 'editor.reviews-edit.click' then 'reviews_block_edited'
           when event = 'editor.banner-edit.click' then 'banner_block_edited'
           when event = 'editor.html-edit.click' then 'html_block_edited'
           when event = 'editor.timer-edit.click' then 'timer_block_edited'
           when event = 'editor.priorityButton-edit.click' then 'priority_button_block_edited'
           when event = 'editor.contactButton-edit.click' then 'button_contact_block_edited'
      end as event,
      block_creation_time,
      block_number,
       utm_source,
        utm_campaign,
        utm_medium,
        initRefferer,
        utm_traffic,
        abtest_name,
        abtest_group,
        abgroup_count_filter,
        devicetypes,
        template_link,
        business_spheres_filter,
        eventgroupname_description,
        business_spheres_filter_description
from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_blocks_created_edits` blocks
join users_stages  on blocks.cmuserId = users_stages.cmuserId