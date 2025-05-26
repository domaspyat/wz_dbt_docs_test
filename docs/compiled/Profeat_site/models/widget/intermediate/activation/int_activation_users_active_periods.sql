with active_periods as (
        select 
                        users.id as user_id,
                        date_trunc(events._ibk,month) active_date,   
                        'monthly' as period_type             
        from `dwh-wazzup`.`dbt_nbespalov`.`stg_users_filtered_from_test` users
        join `dwh-wazzup`.`dbt_nbespalov`.`stg_settings`  settings on users.id = settings.user_id
        join `dwh-wazzup`.`dbt_nbespalov`.`stg_event_activation` events on settings.id = events.settings_id 

        union all

        select 
                        users.id as user_id,
                        date_trunc(events._ibk,week(monday)) active_date,   
                        'weekly' as period_type             
        from `dwh-wazzup`.`dbt_nbespalov`.`stg_users_filtered_from_test` users
        join `dwh-wazzup`.`dbt_nbespalov`.`stg_settings`  settings on users.id = settings.user_id
        join `dwh-wazzup`.`dbt_nbespalov`.`stg_event_activation` events on settings.id = events.settings_id 
)
select *
from active_periods
where date >= '2022-10-01' --примерная дата начала работы продукта