

with int_cmuserid_edited_2_blocks as (
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_edited_2_blocks`
),
int_cmuserid_edited_1_block as (
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_edited_1_block`
),
int_cmuserid_edited_3_blocks as (
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_edited_3_blocks`
)

select int_cmuserid_edited_1_block.cmuserid, 
    int_cmuserid_edited_1_block.edits_datetime as edits_datetime_1_block,
    int_cmuserid_edited_2_blocks.edits_datetime as edits_datetime_2_blocks,
    int_cmuserid_edited_3_blocks.edits_datetime as edits_datetime_3_blocks
from int_cmuserid_edited_1_block
left join int_cmuserid_edited_2_blocks on int_cmuserid_edited_1_block.cmuserid = int_cmuserid_edited_2_blocks.cmuserid
left join int_cmuserid_edited_3_blocks on int_cmuserid_edited_1_block.cmuserid = int_cmuserid_edited_3_blocks.cmuserid