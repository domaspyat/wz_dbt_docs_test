select *
from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_conversions_to_post`
unpivot (
            users for event_name in (paid,posted)
            )