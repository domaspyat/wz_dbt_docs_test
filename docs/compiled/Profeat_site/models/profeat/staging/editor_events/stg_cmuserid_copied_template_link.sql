with registration_data as (
    select cmuserid,
            registration_datetime from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template` 
)
select all_data.cmuserid,min(datetime) as copied_datetime 
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test` all_data
join registration_data on all_data.cmuserid = registration_data.cmuserid
where
    event in (
        'editor-link.copy.click-new',
        'editor-link.copy.click',
        'qr.download-qrcode',
        'qr.download-qrcode-card',
        'my-sites.copy-link-icon.click',
        'editor-link.share-vk.click',
        'editor-link.share-tg.click',
        'editor-link.share-inst.click',
        'editor-link.share-tiktok.click',
        'editor-link.share-fb.click',
        'editor-link.share-tw.click',
        'editor-link.share-mobile.click'
    )
and all_data.datetime >= registration_datetime
group by all_data.cmuserid