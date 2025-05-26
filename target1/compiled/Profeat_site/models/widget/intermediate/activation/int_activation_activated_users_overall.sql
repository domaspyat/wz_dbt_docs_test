with first_date_defining as 
                              (select 
                                    distinct
                                     users.id as user_id,
                                    first_value(events.created_at) over (partition by users.id,wazzup_id order by events.created_at) first_time_value_over_userid
                                from `dwh-wazzup`.`dbt_nbespalov`.`stg_users_filtered_from_test` users
                                join `dwh-wazzup`.`dbt_nbespalov`.`stg_settings`  settings on users.id = settings.user_id
                                join `dwh-wazzup`.`dbt_nbespalov`.`stg_event_activation` events on settings.id = events.settings_id 
                          )
,ranks as (
            select *,
                    dense_rank() over (partition by user_id order by first_time_value_over_userid) rank
            from first_date_defining
            )
select * except(rank)
from ranks
where rank = 3