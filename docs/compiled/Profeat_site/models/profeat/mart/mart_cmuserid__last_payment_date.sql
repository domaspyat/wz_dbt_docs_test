select
    ps.cmuserid,
    template_link,
    phone,
    ps.last_payment_date,
    pu.datetime as unsubscribe_date
from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template` ps
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_unsubscribe` pu using (cmuserid)
where (last_payment_date is not null) or (pu.datetime is not null)