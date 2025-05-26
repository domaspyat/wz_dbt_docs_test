select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_sites_data` 
where (phone not in (select * from `dwh-wazzup`.`dbt_nbespalov`.`test_phones`) or phone is null)
and (cmuserid not in (select * from  `dwh-wazzup`.`dbt_nbespalov`.`test_cmuserid`) or cmuserid is null)
and (cmuserid not in (select distinct cmuserid
                    from `dwh-wazzup`.`dbt_nbespalov`.`test_phones` phones
                    join `dwh-wazzup`.`dbt_nbespalov`.`stg_sites_data`  all_data  on phones.phones = all_data.phone 
                    where cmuserid is not null) or cmuserId is null)