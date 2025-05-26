with int_cmuserid_conversions_to_post as (
select *except(posted),
        case when datetime = first_payment_datetime and event like '%payment.success-max_1_month%' then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as first_max_one_month_cmuserid,
        case when datetime != first_payment_datetime and event like '%payment.success-max_1_month%' then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as repeat_max_one_month_cmuserid,
        case when  event like '%payment.success.recurring-max_1_month%' then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as recurring_max_one_month_cmuserid,
        case when datetime = first_payment_datetime and event like '%payment.success-max_1_year%' then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as first_max_twelve_month_cmuserid,
        case when datetime != first_payment_datetime and event like '%payment.success-max_1_year%' then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as repeat_max_twelve_month_cmuserid,
        case when  event like '%payment.success.recurring-max_1_year%' then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as recurring_max_twelve_month_cmuserid,


        case when datetime = first_payment_datetime and event like '%payment.success-pro_1_month%' then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as first_pro_one_month_cmuserid,
        case when datetime != first_payment_datetime and event like '%payment.success-pro_1_month%' then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as repeat_pro_one_month_cmuserid,
        case when  event like '%payment.success.recurring-pro_1_month%' then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as recurring_pro_one_month_cmuserid,
        
         case when datetime = first_payment_datetime and event like '%payment.success-pro_1_year%' then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as first_pro_twelve_month_cmuserid,
        case when datetime != first_payment_datetime and event like '%payment.success-pro_1_year%' then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as repeat_pro_twelve_month_cmuserid,
        case when  event like '%payment.success.recurring-pro_1_year%' then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as recurring_pro_twelve_month_cmuserid,
        case when  datetime = first_payment_datetime and event in('payment.success.recurring','payment.success') then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as old_payments_new,
        case when  datetime != first_payment_datetime and event in('payment.success.recurring','payment.success') then concat(dense_rank() over (partition by cmuserid order by datetime),cmuserid) end as old_payments_repeat

        from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_conversions_to_post`
        where paid is not null
        and (event like '%payment.success%')
)
select distinct *--except(payment_sum),case when  row_number () over (partition by cmuserid,datetime order by datetime) = 1  then cast(payment_sum as int) else null end as payment_sum
from int_cmuserid_conversions_to_post
unpivot (
            users for event_name in (first_max_one_month_cmuserid,
                                    repeat_max_one_month_cmuserid,
                                    recurring_max_one_month_cmuserid,

                                    first_max_twelve_month_cmuserid,
                                    repeat_max_twelve_month_cmuserid,
                                    recurring_max_twelve_month_cmuserid,
                                    
                                    first_pro_one_month_cmuserid,
                                    repeat_pro_one_month_cmuserid,
                                    recurring_pro_one_month_cmuserid,

                                    first_pro_twelve_month_cmuserid,
                                    repeat_pro_twelve_month_cmuserid,
                                    recurring_pro_twelve_month_cmuserid,

                                    old_payments_new,
                                    old_payments_repeat
                                    )
            )