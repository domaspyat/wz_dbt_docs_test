with
    reg_data as (
        select
            cmuserid,
            registration_date,
            utm_campaign,
            utm_source,
            utm_medium,
            utm_traffic
        from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template`
    ),
registration_trials_and_payments_tries as (
select
    cmuserid,
    event,
    datetime,
    date,
    null as payment_success_count,
    lag(datetime) over (partition by cmuserid order by datetime) as next_date
from `dwh-wazzup`.`mongo_db`.`df_events`
where
    event in (
        'payment.attempt',
        'register-confirm-code-success',
        'trial.start',
        'payment.unsubscribe',
        'trial.try-closed',
        'payment.success.recurring'
    )),



payments_regs_trials_union as (
        select *
        from registration_trials_and_payments_tries

        union all

        select 
        cmuserid,
        'payment.success.all' as event,
        datetime,
        date,
        payment_success_count,
        next_date
        from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_successful__event_count_next_date`

        union all

        select *
        from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_attempts`


        union all
        select  
        cmuserid,
        'payment.success' as event,
        datetime,
        date,
        payment_success_count,
        next_date
        from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_successful__event_count_next_date`)



select
    p.cmuserid,
    p.event,
    p.datetime,
    p.date,
    p.payment_success_count,
    p.next_date,
    ts.try_start,
    reg_data.registration_date,
    reg_data.utm_campaign,
    reg_data.utm_traffic,
    utm_source,
    utm_medium
from reg_data


left join payments_regs_trials_union p using (cmuserid)
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_trial_start` ts using (cmuserid)