with waba_channels as (
    select created_week,
           guid,
           is_new_channel
    from `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary`
    where transport = 'wapi'
    ),
waba_created_per_week as (
    select created_week,
    is_new_channel,
    
    count(guid) as channels_count,
    from waba_channels
    group by 1,2
    )
select * from waba_created_per_week