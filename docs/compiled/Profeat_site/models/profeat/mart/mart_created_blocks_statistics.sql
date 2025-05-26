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
      case when event = 'editor.add.click' then 'block_added'
      when event = 'editor-add.main.click' then 'hat_block_added'
      when event = 'editor-add.text.click' then 'text_block_added'
      when event = 'editor-add.socials.click' then 'socials_block_added'
      when event = 'editor-add.messengers.click' then 'messengers_block_added'
      when event = 'editor-add.link.click' then 'link_block_added'
      when event = 'editor-add.photos.click' then 'photos_block_added'
      when event = 'editor-add.video.click' then 'videos_block_added'
      when event = 'editor-add.separator.click' then 'separator_block_added'
      when event = 'editor-add.price.click' then 'price_block_added'
      when event = 'editor-add.map.click' then 'map_block_added'
      when event = 'editor-add.products.click' then 'products_block_added'
      when event = 'editor-add.faq.click' then 'faq_block_added'
      when event = 'editor-add.reviews.click' then 'reviews_block_added'
      when event = 'editor-add.banner.click' then 'banner_block_added'
      when event = 'editor-add.html.click' then 'html_block_added'
      when event = 'editor-add.timer.click' then 'timer_block_added'
      when event = 'editor-add.priority-button.click' then 'priority_button_block_added'
      when event = 'editor-add.button-contact.click' then 'button_contact_block_added'
      end as event,
      block_creation_time,
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
from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_blocks_created` blocks
join users_stages  on blocks.cmuserId = users_stages.cmuserId