with plan_fact_relations as (select * from 

        (
            select
                cast('''`dwh-wazzup`.`profeat_seo`.`profeat_seo_plan_table`''' as string) as _dbt_source_relation,

                
                    cast(`metrics` as STRING) as `metrics` ,
                    cast(`query` as STRING) as `query` ,
                    cast(`_5_1_2023` as INT64) as `_5_1_2023` ,
                    cast(`_6_1_2023` as INT64) as `_6_1_2023` ,
                    cast(`_7_1_2023` as INT64) as `_7_1_2023` ,
                    cast(`_8_1_2023` as INT64) as `_8_1_2023` ,
                    cast(`_9_1_2023` as INT64) as `_9_1_2023` ,
                    cast(`_10_1_2023` as INT64) as `_10_1_2023` ,
                    cast(`_11_1_2023` as INT64) as `_11_1_2023` ,
                    cast(`_12_1_2023` as INT64) as `_12_1_2023` ,
                    cast(`_1_1_2024` as INT64) as `_1_1_2024` 

            from `dwh-wazzup`.`profeat_seo`.`profeat_seo_plan_table`

            
        )

        union all
        

        (
            select
                cast('''`dwh-wazzup`.`profeat_seo`.`profeat_seo_fact_monthly`''' as string) as _dbt_source_relation,

                
                    cast(`metrics` as STRING) as `metrics` ,
                    cast(`query` as STRING) as `query` ,
                    cast(`_5_1_2023` as INT64) as `_5_1_2023` ,
                    cast(`_6_1_2023` as INT64) as `_6_1_2023` ,
                    cast(`_7_1_2023` as INT64) as `_7_1_2023` ,
                    cast(null as INT64) as `_8_1_2023` ,
                    cast(null as INT64) as `_9_1_2023` ,
                    cast(null as INT64) as `_10_1_2023` ,
                    cast(null as INT64) as `_11_1_2023` ,
                    cast(null as INT64) as `_12_1_2023` ,
                    cast(null as INT64) as `_1_1_2024` 

            from `dwh-wazzup`.`profeat_seo`.`profeat_seo_fact_monthly`

            
        )

        )

select * from plan_fact_relations