with all_successful_payments as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_successful_payments_without_pro_demo` 
    where  sum is not null 
    --paid_date >= '2023-06-01' --примерно в июне начали ко всем платежам добавлять сумму в inner_event.details->>'sum' , до этого там было пусто
            
),
    stg_months as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_months`
),
    getting_first_row_in_a_group as (
select
        paid_date
        ,user_id
        ,duration
        ,duration_period
        ,sum
        ,case   when duration like '%month%' then date_add(paid_date, interval cast(duration_period as int) month )
                when duration like '%days%' then date_add(paid_date, interval cast(duration_period as int) day )
                when duration like '%year%' then date_add(paid_date, interval cast(duration_period as int) year )
         end as till_what_date
        ,months.month as active_month
from all_successful_payments 
join stg_months months on date_trunc(paid_date,month) <= months.month
                      and months.month < date_trunc(case when duration like '%month%' then date_add(paid_date, interval cast(duration_period as int) month )
                                               when duration like '%days%' then date_add(paid_date, interval cast(duration_period as int) day )
                                                when duration like '%year%' then date_add(paid_date, interval cast(duration_period as int) year )
                                        end,month)
    ), --in case there were more than 1 payment in one month
     defining_GMR as (
select distinct active_month
                ,user_id
                ,sum(sum) over (partition by user_id,active_month order by active_month)/count(*) over (partition by user_id,paid_date) as GMR
from getting_first_row_in_a_group
     )
     select active_month,
            user_id,
            MAX(GMR) GMR
     from defining_GMR
     group by active_month,
                user_id