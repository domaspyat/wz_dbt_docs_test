select  all_data.cmuserid, date(dateTime) as date,
datetime,
min(dateTime) over (partition by all_data.cmuserId order by date) promo_tariff_datetime
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test` all_data
join `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template` registration_data on all_data.cmuserid = registration_data.cmuserid
where event in ('promo_tariff.post','promo_tariff')
and all_data.datetime >= registration_data.registration_datetime