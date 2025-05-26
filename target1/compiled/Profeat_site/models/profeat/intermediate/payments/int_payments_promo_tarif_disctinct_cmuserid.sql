select distinct cmuserid,
promo_tariff_datetime as posted_datetime
from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_promo_tariff`