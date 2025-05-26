with blocks_list as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_blocks_list`
),
all_events as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test`
),
defining_next_events as (
select  events.cmuserid,
        datetime as created_at,
        lead(datetime) over (partition by events.cmuserid,firstevent order by datetime) saved_at,
        event,
        lead(event) over (partition by events.cmuserid,firstevent order by datetime) next_event,
        firstevent,
        SecondEvent
from all_events  events
join blocks_list blocks on (events.event = blocks.FirstEvent 
                                    or events.event = blocks.SecondEvent)
), first_created_block as ( 
select cmuserid,
       event,
       min(created_at) as created_at,
       min(saved_at) as saved_at,
       date_diff(min(saved_at),min(created_at),second) as block_creation_time
from defining_next_events
where event = firstevent 
      and next_event = secondevent 
group by 1,2
)
select *,
 percentile_cont(block_creation_time,0.75) over (partition by event) seventy_five_percentile
from first_created_block