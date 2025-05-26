with int_all_filters as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_all_info_that_used_as_filters`
),
payments_table as (
    select payments.datetime payment_datetime,
           payments.date as payment_date,
           payments.first_payment_datetime,
           payments.cmuserid,
           event,
           payment_sum,
           first_value(datetime) over (partition by cmuserid,event order by datetime) first_payment_datetime_in_a_group
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_success_and_recurring` payments
)
, post_conversions as (
    select 
           promo_tariff_datetime as posted_datetime,
           cmuserid,
         'promo_tariff' as event
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_promo_tariff`
   
),posts_and_payments as (
     select 
           payments_table.cmuserid cmuserid,
           first_payment_datetime,
           first_payment_datetime_in_a_group,
           event,
           payment_sum,
           payment_datetime datetime,
           payments_table.cmuserid paid,
           null posted
    from payments_table
    union all
    select 
            cmuserid cmuserid,
           null as first_payment_datetime,
           null as first_payment_datetime_in_a_group,
           event,
           null as payment_sum,
           posted_datetime,
           null as paid,
           cmuserid posted
    from post_conversions
  )
select posts_and_payments.*,
      registrations.* except(cmuserid)
from posts_and_payments
join int_all_filters registrations on posts_and_payments.cmuserid = registrations.cmuserid