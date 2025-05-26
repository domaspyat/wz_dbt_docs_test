with blocks_list as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_blocks_list_edits`
),
all_events as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test`
),
defining_next_events as (
select  events.cmuserid,
        datetime as created_at,
        lead(datetime) over (partition by events.cmuserid,first_event order by datetime) saved_at,
        event,
        lead(event) over (partition by events.cmuserid,first_event order by datetime) next_event,
        first_event,
        Second_Event
from all_events  events
join blocks_list blocks on (events.event = blocks.First_Event 
                                    or events.event = blocks.Second_Event)
), created_blocks as ( 
select cmuserid,
       event,
      created_at as created_at,
       saved_at as saved_at,
       date_diff(saved_at,created_at,second) as block_creation_time
from defining_next_events
where event = first_event 
      and next_event = second_event 
)
select 
*,
row_number() over (partition by cmuserid,event) as block_number
from created_blocks