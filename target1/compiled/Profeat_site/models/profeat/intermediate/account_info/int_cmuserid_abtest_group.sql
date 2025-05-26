WITH all_data AS (
  select cmuserid,
  abTestsGroup,
  profeat.jsonObjectKeys(abTestsGroup) key,
  profeat.jsonObjectValues(abTestsGroup) values,
  datetime
  from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test`
  where abTestsGroup is not null
  and cmuserid is not null
  and event is distinct from 'visitka-enter'
), 
keys as (
SELECT distinct cmuserid,
            k,
            offset
FROM all_data,unnest(all_data.key) k
WITH OFFSET AS offset
),
values as (
  SELECT distinct all_data.cmuserid,
                    k as abtest_name,
                    v as abtest_group
  FROM all_data,unnest(all_data.values) v
  WITH OFFSET AS offset
  join keys on all_data.cmuserid = keys.cmuserid
                and OFFSET = keys.offset 
),
count_abgroup as (
SELECT distinct cmuserid,
                abtest_name,
                abtest_group,
                count(distinct abtest_group) over (partition by cmuserid,abtest_name) abgroup_count
FROM values
where abtest_group !='nan'
)
select *
from count_abgroup