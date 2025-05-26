with int_funnel_key_events__finding_all_users_stages_time as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_funnel_key_events__finding_all_users_stages_time`
),
overall_time_calculation as (
    select *,
    date_diff(greatest(coalesce(templateusage,registration_datetime),
                 coalesce(edits,registration_datetime),
                 coalesce(copies,registration_datetime),
                 coalesce(activation,registration_datetime),
                 coalesce(posted,registration_datetime),
                 coalesce(paid,registration_datetime),
                 coalesce(edits_one_block,registration_datetime),
                 coalesce(edits_three_blocks,registration_datetime),
                 coalesce(activation_one_client,registration_datetime),
                 coalesce(activation_five_clients,registration_datetime),
                 coalesce(activation_ten_clients,registration_datetime)
                 ),
                 registration_datetime,
                 minute
                 ) as overall_time
        from int_funnel_key_events__finding_all_users_stages_time
)
select * 
from overall_time_calculation